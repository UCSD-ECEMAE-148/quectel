import logging
import time

from fusion_engine_client.messages import *
from fusion_engine_client.parsers import FusionEngineEncoder, FusionEngineDecoder

from p1_runner.nmea_framer import NMEAFramer

logger = logging.getLogger('point_one.device_interface')

RESPONSE_TIMEOUT = 5


class DeviceInterface:
    def __init__(self, serial_out, serial_in, timeout=RESPONSE_TIMEOUT):
        self.serial_out = serial_out
        self.serial_in = serial_in
        self.timeout = timeout
        # size of UserConfig is 1236 bytes
        self.fe_decoder = FusionEngineDecoder(2046, warn_on_unrecognized=False, return_bytes=True)
        self.fe_encoder = FusionEngineEncoder()
        self.nmea_framer = NMEAFramer()

    def set_config(self, config_object, save=False):
        config_set_cmd = SetConfigMessage()
        if save:
            config_set_cmd.flags = SetConfigMessage.FLAG_APPLY_AND_SAVE
        config_set_cmd.config_object = config_object
        message = self.fe_encoder.encode_message(config_set_cmd)

        logger.debug('Sending config to device. [size=%d B]' % len(message))
        self._send(message)

    def send_save(self):
        apply_cmd = SaveConfigMessage()
        message = self.fe_encoder.encode_message(apply_cmd)
        logger.debug('Saving config. [size=%d B]' % len(message))
        self._send(message)

    def get_config(self, source: ConfigurationSource, config_type: ConfigType):
        req_cmd = GetConfigMessage()
        req_cmd.config_type = config_type
        req_cmd.request_source = source
        message = self.fe_encoder.encode_message(req_cmd)

        logger.debug('Requesting config. [size=%d B]' % len(message))
        self._send(message)

    def get_message_rate(self, source: ConfigurationSource, config_object):
        config_object.insert(2, source)
        req_cmd = GetMessageRate(*config_object)
        message = self.fe_encoder.encode_message(req_cmd)

        logger.debug('Querying message rate. [size=%d B]' % len(message))
        self._send(message)

    def set_message_rate(self, config_object):
        config_set_cmd = SetMessageRate(*config_object)
        message = self.fe_encoder.encode_message(config_set_cmd)

        logger.debug('Sending message rate config to device. [size=%d B]' % len(message))
        self._send(message)

    def send_message(self, message: Union[MessagePayload, str]):
        if isinstance(message, MessagePayload):
            encoded_data = self.fe_encoder.encode_message(message)
            logger.debug('Sending %s message. [size=%d B]' % (repr(message), len(encoded_data)))
            self._send(encoded_data)
        else:
            if message[0] != '$':
                message = '$' + message
            message += '*%02X' % NMEAFramer._calculate_checksum(message)
            encoded_data = (message + '\r\n').encode('utf8')
            logger.debug('Sending NMEA message. [%s (%d B)]' % (message.rstrip(), len(encoded_data)))
            self._send(encoded_data)

    def wait_for_message(self, msg_type):
        if isinstance(msg_type, MessageType):
            return self._wait_for_fe_message(msg_type)
        else:
            return self._wait_for_nmea_message(msg_type)

    def _wait_for_fe_message(self, msg_type):
        start_time = time.time()
        while True:
            msgs = self.fe_decoder.on_data(self.serial_in.read(1))
            for msg in msgs:
                if msg[0].message_type == msg_type:
                    logger.debug('Response: %s', str(msg[1]))
                    logger.debug(' '.join('%02x' % b for b in msg[2]))
                    return msg[1]
            if time.time() - start_time > self.timeout:
                return None

    def _wait_for_nmea_message(self, msg_type):
        if msg_type[0] != '$':
            msg_type = '$' + msg_type

        start_time = time.time()
        while True:
            msgs = self.nmea_framer.on_data(self.serial_in.read(1))
            for msg in msgs:
                if msg.startswith(msg_type):
                    msg = msg.rstrip()
                    logger.debug('Response: %s', msg)
                    return msg
            if time.time() - start_time > self.timeout:
                return None

    def _send(self, message):
        logger.debug(' '.join('%02x' % b for b in message))
        self.serial_out.write(message)
