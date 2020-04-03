"""Connect two Home Assistant instances via MQTT."""
import asyncio
import json
import logging
import re

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.mqtt import (
    ATTR_TOPIC,
    valid_publish_topic,
    valid_subscribe_topic,
)
from homeassistant.const import (
    ATTR_DOMAIN,
    ATTR_ENTITY_ID,
    ATTR_SERVICE,
    ATTR_SERVICE_DATA,
    EVENT_CALL_SERVICE,
    EVENT_SERVICE_REGISTERED,
    EVENT_STATE_CHANGED,
    EVENT_TIME_CHANGED,
    MATCH_ALL,
)
from homeassistant.core import EventOrigin, State, callback
from homeassistant.helpers.json import JSONEncoder

_LOGGER = logging.getLogger(__name__)

DOMAIN = "mqtteventstream"

QOS_AT_MOST_ONCE = 0
QOS_AT_LEAST_ONCE = 1
QOS_EXACTLY_ONCE = 2

ATTR_ATTRIBUTES = "attributes"
ATTR_EVENT_TYPE = "event_type"
ATTR_EVENT_DATA = "event_data"
ATTR_EVENT_ORIGIN = "origin"
ATTR_NEW_STATE = "new_state"
ATTR_OLD_STATE = "old_state"
ATTR_SOURCE = "source"
ATTR_ROUTE = "route"
ATTR_METHOD = 'method'
ATTR_INSTANCE_NAME = 'instance_name'


CONF_PUBLISH_TOPIC = "publish_topic"
CONF_STATE_PUBLISH_TOPIC = "state_publish_topic"
CONF_ROUTE_PUBLISH_TOPIC = "route_publish_topic"
CONF_SUBSCRIBE_STATE_TOPIC = "subscribe_state_topic"
CONF_SUBSCRIBE_TOPIC = "subscribe_topic"
CONF_SUBSCRIBE_RULES_TOPIC = "subscribe_rules_topic"
CONF_IGNORE_EVENT = "ignore_event"
CONF_IGNORE_EVENT_DATA_PATTERNS = "ignore_event_data_patterns"

EVENT_PUBLISH_STATES = "publish_states"
EVENT_TYPE_ROUTE_REGISTERED = 'route_registered'

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Optional(CONF_PUBLISH_TOPIC): valid_publish_topic,
                vol.Optional(CONF_STATE_PUBLISH_TOPIC): valid_publish_topic,
                vol.Optional(CONF_ROUTE_PUBLISH_TOPIC): valid_publish_topic,
                vol.Optional(CONF_SUBSCRIBE_RULES_TOPIC): valid_subscribe_topic,
                vol.Optional(CONF_SUBSCRIBE_STATE_TOPIC): valid_subscribe_topic,
                vol.Optional(CONF_SUBSCRIBE_TOPIC): valid_subscribe_topic,
                vol.Optional(CONF_IGNORE_EVENT, default=[]): vol.All(
                    cv.ensure_list,
                    [str]),
                vol.Optional(CONF_IGNORE_EVENT_DATA_PATTERNS, default=[]): vol.All(
                    cv.ensure_list,
                    [vol.All(vol.Coerce(str), re.compile)]
                )
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


# pylint: disable=R0914
# pylint: disable=R0915
@asyncio.coroutine
def async_setup(hass, config):
    """Set up the MQTT eventstream component."""
    mqtt = hass.components.mqtt
    conf = config.get(DOMAIN, {})
    pub_topic = conf.get(CONF_PUBLISH_TOPIC, None)
    sub_topic = conf.get(CONF_SUBSCRIBE_TOPIC, None)
    state_sub_topic = conf.get(CONF_SUBSCRIBE_STATE_TOPIC, None)
    state_pub_topic = conf.get(CONF_STATE_PUBLISH_TOPIC, None)
    route_pub_topic = conf.get(CONF_ROUTE_PUBLISH_TOPIC, None)
    rules_sub_topic = conf.get(CONF_SUBSCRIBE_RULES_TOPIC, None)
    ignore_event = conf.get(CONF_IGNORE_EVENT, [])
    ignore_event_data_patterns = conf.get(CONF_IGNORE_EVENT_DATA_PATTERNS, [])

    def _should_ignore(event_data):
        for pattern in ignore_event_data_patterns:
            if pattern.match(event_data):
                return True

        return False

    @callback
    def _event_publisher(event):
        """Handle events by publishing them on the MQTT queue."""
        if event.origin != EventOrigin.local:
            return
        if event.event_type == EVENT_TIME_CHANGED \
                or event.event_type in ignore_event \
                or _should_ignore(event.data):
            return

        # Filter out the events that were triggered by publishing
        # to the MQTT topic, or you will end up in an infinite loop.
        if event.event_type == EVENT_CALL_SERVICE:
            if (
                    event.data.get(ATTR_DOMAIN) == mqtt.DOMAIN
                    and event.data.get(ATTR_SERVICE) == mqtt.SERVICE_PUBLISH
                    and event.data[ATTR_SERVICE_DATA].get(ATTR_TOPIC) == pub_topic
            ):
                return

        event_info = {
            ATTR_EVENT_TYPE: event.event_type,
            ATTR_EVENT_DATA: event.data
        }
        msg = json.dumps(event_info, cls=JSONEncoder)

        special_publish_events = {
            EVENT_STATE_CHANGED: "%s/%s" % (
                state_pub_topic if state_pub_topic else pub_topic,
                event.data.get(ATTR_ENTITY_ID)),
            EVENT_TYPE_ROUTE_REGISTERED: "%s/%s/%s/%s" % (
                route_pub_topic if route_pub_topic else pub_topic,
                event.data.get(ATTR_INSTANCE_NAME),
                event.data.get(ATTR_METHOD),
                event.data.get(ATTR_ROUTE))
        }

        topic = special_publish_events.get(event.event_type)
        if not topic:
            mqtt.async_publish(pub_topic, msg)
        else:
            mqtt.async_publish(topic, msg, QOS_EXACTLY_ONCE, True)

    # Only listen for local events if you are going to publish them.
    if pub_topic:
        hass.bus.async_listen(MATCH_ALL, _event_publisher)

    @callback
    def _publish_state(state):
        message = {
            ATTR_EVENT_TYPE: EVENT_STATE_CHANGED,
            ATTR_EVENT_DATA: {
                ATTR_OLD_STATE: state.as_dict(),
                ATTR_NEW_STATE: state.as_dict()
            },
            ATTR_EVENT_ORIGIN: EventOrigin.local
        }
        mqtt.async_publish(state_pub_topic + "/" + state.entity_id,
                           json.dumps(message, cls=JSONEncoder), QOS_EXACTLY_ONCE, True)

    @callback
    def _publish_states():
        for state in hass.states.all():
            _publish_state(state)

    @callback
    def _handle_remote_state_change(event_data):
        for key in (ATTR_OLD_STATE, ATTR_NEW_STATE):
            state_item = State.from_dict(event_data.get(key))

            if state_item:
                event_data[key] = state_item
        entity_id = event_data.get(ATTR_ENTITY_ID)
        new_state = event_data.get(ATTR_NEW_STATE, {})

        if new_state:
            hass.states.async_set(
                entity_id,
                new_state.state,
                new_state.attributes
            )
            return

    # Process events from a remote server that are received on a queue.
    @callback
    def _event_receiver(msg):
        """Receive events published by and fire them on this hass instance."""
        event = json.loads(msg.payload)
        event_type = event.get(ATTR_EVENT_TYPE)
        event_data = event.get(ATTR_EVENT_DATA)

        if not event_type:
            return

        if event_type == EVENT_PUBLISH_STATES and state_pub_topic:
            hass.add_job(_publish_states())
            return

        # Special case handling for event STATE_CHANGED
        # We will try to convert state dicts back to State objects
        # Copied over from the _handle_api_post_events_event method
        # of the api component.

        if EVENT_STATE_CHANGED == event_type and not state_sub_topic:
            _handle_remote_state_change(event_data)
            return

        if event_type == EVENT_CALL_SERVICE:
            hass.loop.create_task(hass.services.async_call(
                event_data.get(ATTR_DOMAIN),
                event_data.get(ATTR_SERVICE),
                event_data.get(ATTR_SERVICE_DATA, {})))

        elif event_type == EVENT_SERVICE_REGISTERED:
            domain = event_data.get(ATTR_DOMAIN)
            service = event_data.get(ATTR_SERVICE)
            if not hass.services.has_service(domain, service):
                hass.services.async_register(
                    domain,
                    service,
                    lambda svc: _LOGGER.info("Calling remote service %s on domain %s",
                                             service,
                                             domain)
                )

        hass.bus.async_fire(
            event_type, event_data=event_data, origin=EventOrigin.remote
        )

    # Only subscribe if you specified a topic
    if sub_topic:
        yield from mqtt.async_subscribe(sub_topic, _event_receiver)

    if rules_sub_topic:
        yield from mqtt.async_subscribe(rules_sub_topic, _event_receiver)

    # Process events from a remote server that are received on a queue.
    @callback
    def _state_receiver(msg):
        """Receive states published by and fire them on this hass instance."""
        event = json.loads(msg.payload)
        event_type = event.get(ATTR_EVENT_TYPE)
        event_data = event.get(ATTR_EVENT_DATA)

        if event_type != EVENT_STATE_CHANGED:
            return

        _handle_remote_state_change(event_data)

    if state_sub_topic:
        yield from mqtt.async_subscribe(state_sub_topic, _state_receiver)

    return True
