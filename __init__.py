"""Connect two Home Assistant instances via MQTT."""
import copy
import json
import logging
import re

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.mqtt import (
    DOMAIN as MQTT_DOMAIN,
    SERVICE_PUBLISH as MQTT_SERVICE_PUBLISH,
    valid_publish_topic,
    valid_subscribe_topic,
)
from homeassistant.components.system_log import EVENT_SYSTEM_LOG
from homeassistant.const import (
    ATTR_DOMAIN,
    ATTR_ENTITY_ID,
    ATTR_SERVICE,
    ATTR_SERVICE_DATA,
    EVENT_CALL_SERVICE,
    EVENT_SERVICE_REGISTERED,
    EVENT_PLATFORM_DISCOVERED,
    EVENT_COMPONENT_LOADED,
    EVENT_STATE_CHANGED,
    EVENT_TIME_CHANGED,
    EVENT_LOGBOOK_ENTRY,
    EVENT_CORE_CONFIG_UPDATE,
    EVENT_HOMEASSISTANT_CLOSE,
    EVENT_HOMEASSISTANT_START,
    EVENT_AUTOMATION_TRIGGERED,
    EVENT_SCRIPT_STARTED,
    EVENT_HOMEASSISTANT_STOP,
    EVENT_THEMES_UPDATED,
    EVENT_SERVICE_REMOVED,
    EVENT_TIMER_OUT_OF_SYNC,
    MATCH_ALL,
)
from homeassistant.core import EventOrigin, Event, callback
from homeassistant.helpers.entity_registry import EVENT_ENTITY_REGISTRY_UPDATED
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

EVENT_PANELS_UPDATED = 'panels_updated'
EVENT_LOVELACE_UPDATED = 'lovelace_updated'
EVENT_PUBLISH_STATES = "publish_states"
EVENT_TYPE_ROUTE_REGISTERED = 'route_registered'
EVENT_SERVICE_EXECUTED = 'service_executed'

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
                    [str]
                )
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)

ALWAYS_IGNORED_EVENTS = [
    EVENT_PLATFORM_DISCOVERED,
    EVENT_COMPONENT_LOADED,
    EVENT_TIME_CHANGED,
    EVENT_LOGBOOK_ENTRY,
    EVENT_CORE_CONFIG_UPDATE,
    EVENT_HOMEASSISTANT_CLOSE,
    EVENT_HOMEASSISTANT_START,
    EVENT_AUTOMATION_TRIGGERED,
    EVENT_SCRIPT_STARTED,
    EVENT_HOMEASSISTANT_STOP,
    EVENT_THEMES_UPDATED,
    EVENT_SERVICE_REMOVED,
    EVENT_TIMER_OUT_OF_SYNC,
    EVENT_SYSTEM_LOG,
    EVENT_PANELS_UPDATED,
    EVENT_LOVELACE_UPDATED,
    EVENT_ENTITY_REGISTRY_UPDATED,
    EVENT_ENTITY_REGISTRY_UPDATED
]


def _mqtt_payload_to_event(msg):
    return json.loads(msg.payload)


def _event_to_mqtt_payload(event):
    return json.dumps(event, cls=JSONEncoder)


def _state_to_event(new_state, old_state=None):
    if new_state is None:
        return None
    return Event(
        event_type=EVENT_STATE_CHANGED,
        data={
            ATTR_ENTITY_ID: new_state.entity_id,
            ATTR_OLD_STATE: old_state.as_dict() if old_state is not None else None,
            ATTR_NEW_STATE: new_state.as_dict()
        },
        origin=EventOrigin.remote
    )


# pylint: disable=R0914
# pylint: disable=R0915
async def async_setup(hass, config):
    """Set up the MQTT Event Stream component."""
    mqtt = hass.components.mqtt
    conf = config.get(DOMAIN, {})

    if DOMAIN not in hass.data:
        event_stream = MqttEventStream(hass, mqtt, conf)
        hass.data[DOMAIN] = event_stream

    return await hass.data[DOMAIN].initialize_if_connected()


class MqttEventStream:
    """Class that listens for and sends out events to a message queue broker."""

    def __init__(self, hass, mqtt, config):
        """Create an MQTT Event Stream."""
        self._hass = hass
        self._mqtt = mqtt
        self._config = config

    async def initialize_if_connected(self):
        """Set up subscription and publish topics if MQTT is connected."""
        if not self._hass.data[MQTT_DOMAIN].connected:
            _LOGGER.warning('MQTT is not connected, will try again shortly.')
            return False

        if self.event_publish_topic:
            self._hass.bus.async_listen(
                MATCH_ALL,
                self.publish_event)

        # Only subscribe if you specified a topic
        if self.event_subscribe_topic:
            await self._mqtt.async_subscribe(
                self.event_subscribe_topic,
                self.receive_remote_event)

        if self.rules_engine_subscribe_topic:
            await self._mqtt.async_subscribe(
                self.rules_engine_subscribe_topic,
                self.receive_remote_event)

        if self.state_subscribe_topic:
            await self._mqtt.async_subscribe(
                self.state_subscribe_topic,
                self.receive_state_change)

        return True

    def _should_ignore(self, event_data):
        try:
            json_data = json.dumps(event_data)
            for pattern in self.patterns_to_ignore:
                if re.match(pattern, json_data):
                    return True
        except Exception as err:
            _LOGGER.debug(str(err))
        return False

    def _is_known_entity(self, entity_id):
        return self._hass.states.get(entity_id) is not None

    @property
    def event_publish_topic(self):
        """NQTT topic for which events on the bus are also publisehed to."""
        return self._config.get(CONF_PUBLISH_TOPIC, None)

    @property
    def event_subscribe_topic(self):
        """Main MQTT topic that contains events that may be pertinent to this instance."""
        return self._config.get(CONF_SUBSCRIBE_TOPIC, None)

    @property
    def state_subscribe_topic(self):
        """MQTT topic to listen for remote state changes."""
        return self._config.get(CONF_SUBSCRIBE_STATE_TOPIC, None)

    @property
    def state_publish_topic(self):
        """Get the base MQTT topic where this entity's states are published to"""
        return self._config.get(CONF_STATE_PUBLISH_TOPIC, None)

    @property
    def route_publish_topic(self):
        """Get the MQTT topic where API and HTTP Views are published to."""
        return self._config.get(CONF_ROUTE_PUBLISH_TOPIC, None)

    @property
    def rules_engine_subscribe_topic(self):
        """Get the MQTT topic where the rules engine events are published to."""
        return self._config.get(CONF_SUBSCRIBE_RULES_TOPIC, None)

    @property
    def events_to_ignore(self):
        """List of event types to ignore."""
        return ALWAYS_IGNORED_EVENTS + self._config.get(CONF_IGNORE_EVENT, [])

    @property
    def patterns_to_ignore(self):
        """List of event data patterns to ignore."""
        return self._config.get(CONF_IGNORE_EVENT_DATA_PATTERNS, [])

    async def receive_state_change(self, msg):
        """Handle entity state change on a remote source."""
        try:
            event = _mqtt_payload_to_event(msg)
        except Exception as err:
            _LOGGER.error(str(err))
            return

        event_type = event.get(ATTR_EVENT_TYPE, None)
        if event_type is None:
            return

        event_data = event.get(ATTR_EVENT_DATA, {})
        new_state = event_data.get(ATTR_NEW_STATE, None)
        entity_id = event_data.get(
            ATTR_ENTITY_ID,
            event_data).get(ATTR_ENTITY_ID)

        if new_state is None or entity_id is None:
            _LOGGER.warning("Unable to process remote state change event due to missing properties")
            return

        await self._hass.states.async_set(
            entity_id,
            new_state.state,
            new_state.attributes or {}
        )

        await self._hass.bus.async_fire(
            event_type=event_type,
            event_data=event_data,
            origin=EventOrigin.remote
        )

    @callback
    def publish_all_states(self, ):
        """Publish all states to MQTT broker."""
        for state in self._hass.states.all():
            self._publish_state(_state_to_event(state))

    @callback
    def receive_remote_event(self, msg):
        """Handle an event received from a remote source."""
        try:
            event = _mqtt_payload_to_event(msg)
        except Exception as err:
            _LOGGER.error(str(err))
            return
        event_type = event.get(ATTR_EVENT_TYPE, None)
        # Special case handling for event STATE_CHANGED
        # We will try to convert state dicts back to State objects
        # Copied over from the _handle_api_post_events_event method
        if event_type is None or EVENT_STATE_CHANGED == event_type:
            return

        event_data = event.get(ATTR_EVENT_DATA, {})

        _LOGGER.debug('Received event %s %s',
                      event_type,
                      str(event_data))

        if event_type == EVENT_PUBLISH_STATES and self.state_publish_topic:
            self.publish_all_states()
            return

        if event_type == EVENT_CALL_SERVICE:
            _LOGGER.debug('Got call service')
            if not self._hass.services.has_service(
                    event_data.get(ATTR_DOMAIN),
                    event_data.get(ATTR_SERVICE)):
                _LOGGER.debug('Ignoring %s %s',
                              event_data.get(ATTR_DOMAIN),
                              event_data.get(ATTR_SERVICE))
                return
            service_data = copy.deepcopy(event_data.get(ATTR_SERVICE_DATA, {}))
            original_entities = event_data.get(ATTR_SERVICE_DATA, {}).get(ATTR_ENTITY_ID, [])
            filtered_entities = []
            _LOGGER.debug('Original entity_id: %s',
                          str(original_entities))
            if isinstance(original_entities, str):
                filtered_entities.append(original_entities)
            else:
                filtered_entities = [entity_id for entity_id
                                     in original_entities
                                     if self._is_known_entity(entity_id)]
            _LOGGER.debug('Filtered entity_id: %s',
                          str(filtered_entities))
            if ATTR_ENTITY_ID in service_data:
                service_data[ATTR_ENTITY_ID] = filtered_entities

            self._hass.loop.create_task(
                self._hass.services.async_call(
                    event_data.get(ATTR_DOMAIN),
                    event_data.get(ATTR_SERVICE),
                    service_data))
            return

        if event_type == EVENT_SERVICE_REGISTERED:
            domain = event_data.get(ATTR_DOMAIN)
            service_name = event_data.get(ATTR_SERVICE)
            if domain in self._hass.data and not self._hass.services.has_service(domain,
                                                                                 service_name):
                self._hass.services.async_register(
                    domain,
                    service_name,
                    self._service_callback)
                return

        self._hass.bus.async_fire(
            event_type, event_data=event_data, origin=EventOrigin.remote
        )

    @callback
    def publish_event(self, event):
        """Handle events by publishing them on the MQTT queue."""
        if event.origin != EventOrigin.remote:
            return
        if event.event_type == EVENT_TIME_CHANGED \
                or event.event_type in self.events_to_ignore \
                or self._should_ignore(event.data):
            return

        # Filter out the events that were triggered by publishing
        # to the MQTT topic, or you will end up in an infinite loop.
        if event.event_type == EVENT_CALL_SERVICE:
            if (event.data.get(ATTR_DOMAIN) == MQTT_DOMAIN
                    and event.data.get(ATTR_SERVICE) == MQTT_SERVICE_PUBLISH):
                return

        if event.event_type == EVENT_STATE_CHANGED and self.state_publish_topic is not None:
            self._publish_state(event)
            return

        if event.event_type == EVENT_TYPE_ROUTE_REGISTERED and self.route_publish_topic is not None:
            self._publish_api_route(event)
            return

        self._mqtt.async_publish(
            self.event_publish_topic,
            _event_to_mqtt_payload(event))

    @callback
    def _service_callback(self, service_call):
        self.publish_event(
            Event(
                event_type=EVENT_CALL_SERVICE,
                data={
                    ATTR_DOMAIN: service_call.domain,
                    ATTR_SERVICE: service_call.service,
                    ATTR_SERVICE_DATA: service_call.data or {}
                },
                origin=EventOrigin.remote))

    @callback
    def _publish_api_route(self, event):
        if event is None:
            return

        self._mqtt.async_publish(
            "%s/%s/%s/%s" % (
                self.route_publish_topic,
                event.data.get(ATTR_INSTANCE_NAME),
                event.data.get(ATTR_METHOD),
                event.data.get(ATTR_ROUTE)),
            _event_to_mqtt_payload(event),
            QOS_EXACTLY_ONCE,
            True)

    @callback
    def _publish_state(self, event):
        if event is None:
            return
        self._mqtt.async_publish(
            self.state_publish_topic + "/" + event.data.get(ATTR_ENTITY_ID),
            _event_to_mqtt_payload(event),
            QOS_EXACTLY_ONCE,
            True)
