#!/usr/bin/python3

"""

Object to control retic solenoids

"""

import collections
import threading
import time
import datetime
from time import sleep
import queue
import os
import sys
import pymysql
import pifacedigitalio

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, THIS_DIR)

import log_handler
import get_config



CONFIG_FILE_NAME = 'retic_config.conf'
Solenoid = collections.namedtuple('Solenoid', 'name description pin master')



class EventLogger(object):


    def __write_to_logfile(self, zone, operation, duration):
        
        try:
            logfile = open('{}/{}'.format(THIS_DIR, self.event_logfile), 'a')
            logfile.write("'{}','{}','{}','{}'\r\n".format(str(datetime.datetime.now()), zone, operation, duration))
            logfile.close()
        except Exception as e:
            self.logger.exception('Error writing to file: {}, {}'.format(type(e).__name__, e.args))
            

    def __insert_into_mysql(self, sql_query):
        
        mysql_conn = pymysql.connect(host=self.host, user=self.user, passwd=self.pwd, db=self.db)
        
        try:
            mysql_cursor = mysql_conn.cursor()
            mysql_cursor.execute(sql_query)
            mysql_conn.commit()
            mysql_conn.close()
        except Exception as e:
            self.logger.exception('Error inserting in MySQL: {}, {}'.format(type(e).__name__, e.args))
            mysql_conn.rollback()
            
                    
    def __init__(self, log_handler):
    
        #Open config file
        config = get_config.get_config(CONFIG_FILE_NAME)
        
        self.event_logfile = config.get('Logging', 'event_log_file')
        
        self.logger = log_handler
        
        self.creation_time = time.time()
               
        self.host=config.get('Logging', 'sql_server')
        self.user=config.get('Logging', 'sql_user')
        self.pwd=config.get('Logging', 'sql_pwd')
        self.db=config.get('Logging', 'sql_db')

        
    def log_start_event(self, zone, duration):
        self.__write_to_logfile(zone, 'Start', duration)
        
        sql_query = "INSERT INTO retic_logs (zone, operation, duration, time) VALUES ('{}', 'Start', '{}', now())".format(zone, duration)        
        self.__insert_into_mysql(sql_query)
    
    def log_stop_event(self, zone):
        self.__write_to_logfile(zone, 'Stop', int(time.time()-self.creation_time))
        
        sql_query = "INSERT INTO retic_logs (zone, operation, measured_duration, time) VALUES ('{}', 'Stop', '{}', now())".format(zone, int(time.time()-self.creation_time-4))
        self.__insert_into_mysql(sql_query)



class Sprinkle(object):

    """
    Object to operate sprinkler/solenoid
    """

    def __init__(self, zone_pin, master_pin, duration, log, name):

        self.zone_pin = zone_pin
        self.zone_name = name
        self.master_pin = master_pin
        self.duration = duration
        self.logger2 = log
        self.piface = pifacedigitalio.PiFaceDigital()
        self.stop_event = threading.Event()
        self.complete = False
        
        self.event_log = EventLogger(self.logger2)
        
        self.logger2.debug('New sprinkler job created')

    def __del__(self):
        if not self.complete:
            self.__close_solenoid()
            self.logger2.warning('Sprinkler stopped abnormally')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not self.complete:
            self.__close_solenoid()
            self.logger2.warning('Sprinkler stopped abnormally')
        self.logger2.debug('Sprinkler job ended')


    def get_stop_event(self):
        """
        return stop_event which can be used to close solenoid before
        full duration is complete
        """
        return self.stop_event

    def __open_solenoid(self):
        """Set output pin/s to High to open solenoid/s"""
        self.logger2.debug('Opening valve: %d', self.zone_pin)

        self.piface.leds[self.zone_pin].set_high()

        if self.master_pin:
            sleep(1)
            self.logger2.debug('Opening valve: %d', self.master_pin)
            self.piface.leds[self.master_pin].set_high()

    def __close_solenoid(self):
        """Set output pin/s to Low to close solenoid/s"""

        if self.master_pin:
            self.logger2.debug('Closing valve: %d', self.master_pin)
            self.piface.leds[self.master_pin].set_low()
            sleep(3)

        self.logger2.debug('Closing valve: %d', self.zone_pin)
        self.piface.leds[self.zone_pin].set_low()
        self.complete = True

    def start(self):

        """Open solenoid, wait for duration to complete then close solenoid
        Solenoid can be closed early if a stop_event is set"""

        self.logger2.debug('Sprinkler started: pin %d for %d seconds',
                           self.zone_pin, self.duration)

        self.__open_solenoid()
        
        self.event_log.log_start_event(self.zone_name, self.duration)

        self.stop_event.wait(self.duration)

        self.__close_solenoid()
        
        self.event_log.log_stop_event(self.zone_name)

        if self.stop_event.isSet():
            self.logger2.debug('Sprinkler cancelled')

        self.logger2.debug('Sprinkler stopped: pin %d', self.zone_pin)





class ReticController(object):

    """
    Object provides an interface to the solenoid control
    """

    def __init__(self):

        config = get_config.get_config(CONFIG_FILE_NAME)

        #Setup logging
        self.logger1 = log_handler.get_log_handler(
            'retic_controller_log.txt', 'info', 'retic.controller')
        self.logger2 = log_handler.get_log_handler(
            'retic_controller_log.txt', 'info', 'retic.sprinkler')

        self.logger1.info('Starting: ReticController')

        self.logger1.debug('Getting solenoid config from: %s',
                           config.get('interface', 'solenoid_conf_file'))

        #Get solenoid configuration
        solenoid_config = get_config.get_config(
            config.get('interface', 'solenoid_conf_file'), True)

        self.solenoids = parse_config(solenoid_config)

        self.logger1.debug('Solenoid Configuration:')
        for key in self.solenoids:
            self.logger1.debug('%s: %s', key, str(self.solenoids[key]))


        #Instantiate other variables
        self.instruction_q = queue.Queue()
        self.current_status = None
        self.__reset_current_status()
        self.shutdown_event = threading.Event()

        #Start queue monitor thread
        self.queue_monitor = threading.Thread(
            name='queue_monitor',
            target=self.__monitor_instruction_queue,
            args=(self.shutdown_event,))

        self.queue_monitor.start()


    def __del__(self):
        self.shutdown_event.set()
        
        try:
            with self.instruction_q.mutex:
                self.instruction_q.queue.clear()

            if isinstance(self.current_status['STOP_EVENT'], threading._Event):
                self.current_status['STOP_EVENT'].set()

            self.logger1.info('ReticController has shutdown')
            sleep(1)
        except:
            pass


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown_event.set()
        self.__del__()

    def __set_current_status(self, zone, duration, event):
        """Set current status so this info can be queried"""

        self.current_status = {'ZONE': zone,
                               'DURATION': duration,
                               'STOP_EVENT': event,
                               'START_TIME': time.time(),
                               'JOB_Q_SIZE': self.instruction_q.qsize()}

    def __reset_current_status(self):
        """Set current status to not much when nothing is happening"""
        self.current_status = {'ZONE': '',
                               'DURATION': '',
                               'STOP_EVENT': '',
                               'START_TIME': '',
                               'JOB_Q_SIZE': self.instruction_q.qsize()}


    def __monitor_instruction_queue(self, shutdown_event):

        """Poll instruction queue and execute
        instructions as they are received"""

        self.logger1.debug('Starting queue monitor')

        while not shutdown_event.isSet():

            if not self.instruction_q.empty():

                instruct = self.instruction_q.get()

                zone = instruct['ZONE']
                duration = instruct['DURATION']

                zone_pin = self.solenoids[zone].pin
                zone_name = self.solenoids[zone].name

                if self.solenoids[zone].master:
                    master_pin = self.solenoids[zone].master.pin
                else:
                    master_pin = None

                self.logger1.debug('Received instruction to start %s for %d', zone, duration)

                with Sprinkle(zone_pin, master_pin, duration, self.logger2, zone_name) as retic:

                    self.__set_current_status(zone, duration, retic.get_stop_event())
                    self.logger1.info('STARTED: %s - %d', zone, duration)
                    retic.start()
                    self.logger1.info('STOPPED: %s', zone)
                    self.__reset_current_status()

                self.logger1.debug('Completed instruction to start %s for %d', zone, duration)

            sleep(1)

        self.logger1.debug('Queue monitor has shutdown')


    def get_current_status(self):
        """Return current status"""
        status_dict = self.current_status.copy()
        status_dict.pop('STOP_EVENT', None)
        return status_dict

    def cancel_current_operation(self):
        """Method to stop running operation
        and continue to execute any remaining instructions
        in queue"""

        if self.current_status:
            self.current_status['STOP_EVENT'].set()

    def cancel_all_operations(self):
        """Stop current operation and clear any
        instructions from queue"""

        with self.instruction_q.mutex:
            self.instruction_q.queue.clear()
        self.cancel_current_operation()

    def operate(self, zone, duration):
        """Add an operation to queue"""

        if zone in self.solenoids:
            self.instruction_q.put({'ZONE': zone, 'DURATION': duration})

    def get_stations(self):

        stations = {}
        
        for key in self.solenoids:
            stations[self.solenoids[key].name] = self.solenoids[key].description

        return stations
                
            
    def close(self):
        """Shutdown this interface"""
        self.__del__()

    def shutdown(self):
        """Shutdown this interface"""
        self.__del__()



def parse_config(parser):

    """
    parses config parser object to dictionary of namedtuples
    """

    all_solenoids = {}

    for section_name in parser.sections():

        parser_dict = {}

        for name, value in parser.items(section_name):
            parser_dict[name] = value

        all_solenoids[section_name] = parser_dict

    master_solenoids = {}

    

    #Find master solenoids first
    """for key in all_solenoids:
        print key + ' - ' + str(all_solenoids[key])
        tmp = all_solenoids[key]
        tmp['name'] = tmp.pop('name').upper()

        if (not 'master' in all_solenoids[key] or
                'master' in all_solenoids[key] and
                tmp['master'] not in parser.sections()):

            master_solenoids[key] = Solenoid(name=tmp['name'],
                                  description=tmp['description'],
                                  pin=int(tmp['pin']),
                                  master=None)"""

    
    for key in all_solenoids:
        tmp = all_solenoids[key]
        tmp['name'] = tmp.pop('name').upper()
        
        if 'master' in tmp and tmp['master'] in parser.sections(): 
            tmp2 = all_solenoids[tmp['master']]
            master_solenoids[tmp['master']] = Solenoid(name=tmp2['name'],
                                  description=tmp2['description'],
                                  pin=int(tmp2['pin']),
                                  master=None)

    print(master_solenoids)
    slave_solenoids = {}
    
    #Find solenoids with master solenoids and link to them
    for key in all_solenoids:
        tmp = all_solenoids[key]
        tmp['name'] = tmp.pop('name').upper()

        #if 'master' in all_solenoids[key] and tmp['master'] in parser.sections():

        if key not in master_solenoids:

            if 'master' in tmp:
                master_sol = master_solenoids[tmp['master']]
            else: master_sol = None
            
            slave_solenoids[key] = Solenoid(name=tmp['name'],
                                  description=tmp['description'],
                                  pin=int(tmp['pin']),
                                  master=master_sol)

    #Rename dictionary keys
    for key in slave_solenoids:
        new_key_name = slave_solenoids[key].name
        slave_solenoids[new_key_name] = slave_solenoids.pop(key)


    #print('----------')
    #print(slave_solenoids)
    #print('---------')
    
    return slave_solenoids


if __name__ == "__main__":
    RETIC = ReticController()
    output = RETIC.get_stations()

    print(">------------<")
    print(output)

    RETIC.operate('ZONE1', 10)

    sleep(20)
    
    RETIC.shutdown()
