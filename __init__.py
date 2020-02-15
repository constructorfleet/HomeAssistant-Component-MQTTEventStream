"""Connect two Home Assistant instances via MQTT."""
import asyncio
import json
import logging

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
    ATTR_STATE,
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

ATTR_ATTRIBUTES = "attributes"
ATTR_EVENT_TYPE = "event_type"
ATTR_EVENT_DATA = "event_data"
ATTR_EVENT_ORIGIN = "origin"
ATTR_NEW_STATE = "new_state"
ATTR_OLD_STATE = "old_state"
ATTR_SOURCE = "source"

CONF_STATE_PUBLISH_TOPIC = "state_publish_topic"
CONF_SUBSCRIBE_STATE_TOPIC = "states/#"
CONF_PUBLISH_TOPIC = "publish_topic"
CONF_SUBSCRIBE_TOPIC = "subscribe_topic"
CONF_SUBSCRIBE_RULES_TOPIC = "subscribe_rules_topic"
CONF_IGNORE_EVENT = "ignore_event"

EVENT_PUBLISH_STATES = "publish_states"
EVENT_STATE = "statte"

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Optional(CONF_PUBLISH_TOPIC): valid_publish_topic,
                vol.Optional(CONF_STATE_PUBLISH_TOPIC): valid_publish_topic,
                vol.Optional(CONF_SUBSCRIBE_RULES_TOPIC): valid_subscribe_topic,
                vol.Optional(CONF_SUBSCRIBE_TOPIC): valid_subscribe_topic,
                vol.Optional(CONF_IGNORE_EVENT, default=[]): cv.ensure_list,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


@asyncio.coroutine
def async_setup(hass, config):
    """Set up the MQTT eventstream component."""
    mqtt = hass.components.mqtt
    conf = config.get(DOMAIN, {})
    pub_topic = conf.get(CONF_PUBLISH_TOPIC, None)
    sub_topic = conf.get(CONF_SUBSCRIBE_TOPIC, None)
    state_sub_topic = conf.get(CONF_SUBSCRIBE_STATE_TOPIC, None)
    state_pub_topic = conf.get(CONF_STATE_PUBLISH_TOPIC, None)
    ignore_event = conf.get(CONF_IGNORE_EVENT, [])

    @callback
    def _event_publisher(event):
        """Handle events by publishing them on the MQTT queue."""
        if event.origin != EventOrigin.local:
            return
        if event.event_type == EVENT_TIME_CHANGED:
            return

        # User-defined events to ignore
        if event.event_type in ignore_event:
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

        if event.event_type == EVENT_STATE_CHANGED:
            topic = "%s/%s" % (pub_topic, event.data.get(ATTR_ENTITY_ID))
            mqtt.async_publish(topic, msg, 1, True)
        else:
            mqtt.async_publish(pub_topic, msg)

    # Only listen for local events if you are going to publish them.
    if pub_topic:
        hass.bus.async_listen(MATCH_ALL, _event_publisher)

    # Process events from a remote server that are received on a queue.
    @callback
    def _event_receiver(msg):
        """Receive events published by and fire them on this hass instance."""
        event = json.loads(msg.payload)
        event_type = event.get(ATTR_EVENT_TYPE)
        event_data = event.get(ATTR_EVENT_DATA)

        if event_type == EVENT_PUBLISH_STATES and state_pub_topic:
            for state_item in hass.states.all():
                message = {
                    ATTR_EVENT_TYPE: EVENT_STATE,
                    ATTR_EVENT_DATA: state_item.as_dict(),
                    ATTR_EVENT_ORIGIN: EventOrigin.local
                }
                mqtt.async_publish(state_pub_topic + "/" + state_item.entity_id,
                                   json.dumps(message, cls=JSONEncoder), 1, True)
            return

        # Special case handling for event STATE_CHANGED
        # We will try to convert state dicts back to State objects
        # Copied over from the _handle_api_post_events_event method
        # of the api component.

        if event_type:
            if EVENT_STATE_CHANGED == event_type:
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
                        new_state.attributes,
                        True
                    )
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

    # Process events from a remote server that are received on a queue.
    @callback
    def _state_receiver(msg):
        """Receive states published by and fire them on this hass instance."""
        event = json.loads(msg.payload)
        event_type = event.get(ATTR_EVENT_TYPE)
        event_data = event.get(ATTR_EVENT_DATA)

        if event_type != EVENT_STATE:
            return

        hass.states.set(
            event_data.get(ATTR_ENTITY_ID),
            event_data.get(ATTR_STATE),
            event_data.get(ATTR_ATTRIBUTES, {}),
            force_update=True
        )

    if state_sub_topic:
        yield from mqtt.async_subscribe(state_sub_topic, _state_receiver)

    return True
