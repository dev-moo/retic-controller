#!/usr/bin/python3

"""Retic controller service"""

import os
import socketserver
import json
import sys

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, THIS_DIR)

import retic_controller
import log_handler
import get_config

CONFIG_FILE_NAME = 'retic_config.conf'


class JSONHandler(object):

    """Parse JSON input"""

    def __init__(self, log):
        self.logger = log

        #Instantiate Controller Interface
        self.controller = retic_controller.ReticController()

    def __del__(self):
        self.logger.info('Shutting down JSONHandler')
        self.controller.shutdown()

    def shutdown(self):
        self.__del__()

    def __get_settings(self, cmd):

        try:

            if cmd['TYPE'] == 'STATUS':
                return self.controller.get_current_status()
            elif cmd['TYPE'] == 'STATIONS':
                return self.controller.get_stations()

        except KeyError:
            self.logger.exception('Key error when parsing %s', settings)

        

    def __set(self, settings):

        """Control Interface"""
                
        try:
            if settings['TYPE'] == 'OPERATE':
                self.controller.operate(settings['ZONE'], int(settings['DURATION']))
            elif settings['TYPE'] == 'CANCEL':
                self.controller.cancel_current_operation()
            elif settings['TYPE'] == 'CANCEL_ALL':
                self.controller.cancel_all_operations()

        except KeyError:
            self.logger.exception('Key error when parsing %s', settings)



    def parse(self, cmd):
        """Handle Set and Get operations"""

        command = json.loads(cmd)

        #self.logger.debug('Received %s', command)

        try:
            if 'OPERATION' in command:

                if command['OPERATION'] == 'GET':
                    return json.dumps(self.__get_settings(command))

                elif command['OPERATION'] == 'SET':
                    self.__set(command)

            else:
                self.logger.info('Command contains no Operation: %s', command)

        except KeyError:
            self.logger.exception('Key error when parsing %s', command)

        return json.dumps(command)




class UDPHandler(socketserver.BaseRequestHandler):
    """
    UDPHandler to handle UDP requests
    """

    def __init__(self, request, client_address, srvr):
        self.logger = LOGGER1
        socketserver.BaseRequestHandler.__init__(self, request, client_address, srvr)
        return

    def handle(self):
        data = self.request[0].strip().upper()
        socket = self.request[1]
        data = data.decode()

        self.logger.debug("From %s: %s", self.client_address[0], data)

        try:
            response = JSON_HANDLER.parse(data)
            self.logger.debug("Responding to %s with: %s", self.client_address[0], response)    
            socket.sendto(response.encode(), self.client_address)
        except ValueError:
            self.logger.exception('Exception decoding JSON')



if __name__ == "__main__":

    CONFIG = get_config.get_config(CONFIG_FILE_NAME)

    LOG_FILENAME = THIS_DIR + (lambda: '/' if os.name == 'posix' else '\\')() + CONFIG.get('Server', 'logfile')
    HOST = CONFIG.get('Server', 'server_ip')
    PORT = int(CONFIG.get('Server', 'server_port'))

    LOGGER1 = log_handler.get_log_handler(LOG_FILENAME, 'debug', 'server.UDPHandler')

    JSON_HANDLER = JSONHandler(log_handler.get_log_handler(LOG_FILENAME,
                                                           'debug',
                                                           'server.JSONParser'))

    LOGGER1.info('Starting UPD server at %s:%d', HOST, PORT)
    SERVER = socketserver.UDPServer((HOST, PORT), UDPHandler)
    SERVER.allow_reuse_address = True

    try:
        SERVER.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        JSON_HANDLER.shutdown()
        SERVER.shutdown()
        SERVER.server_close()
        raise

    LOGGER1.info('UDP SocketServer has shutdown')
