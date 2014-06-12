"""
@package mi.instrument.mclane.rasfl.pps.driver
@file marine-integrations/mi/instrument/mclane/ras/ooicore/driver.py
@author Dan Mergens
@brief Driver for the PPSDN
Release notes:

initial version
"""
__author__ = 'Dan Mergens'
__license__ = 'Apache 2.0'

import re

from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.log import get_logger
from mi.core.instrument.protocol_param_dict import \
    ParameterDictType, \
    ParameterDictVisibility
from mi.instrument.mclane.driver import \
    NEWLINE, \
    ProtocolEvent, \
    Parameter, \
    Prompt, \
    McLaneCommand, \
    McLaneResponse, \
    McLaneDataParticleType, \
    McLaneSampleDataParticle, \
    McLaneProtocol, \
    ProtocolState

log = get_logger()

NUM_PORTS = 24  # number of collection filters

####
#    Driver Constant Definitions
####


FLUSH_VOLUME = 150
FLUSH_RATE = 100
FLUSH_MIN_RATE = 75
FILL_VOLUME = 4000
FILL_RATE = 100
FILL_MIN_RATE = 75
CLEAR_VOLUME = 100
CLEAR_RATE = 100
CLEAR_MIN_RATE = 75


class Command(McLaneCommand):
    """
    Instrument command strings - case insensitive
    """
    CAPACITY = 'capacity'  # pump max flow rate mL/min


class Response(McLaneResponse):
    """
    Expected device response strings
    """
    # e.g. 03/25/14 20:24:02 PPS ML13003-01>
    READY = re.compile(r'(\d+/\d+/\d+\s+\d+:\d+:\d+\s+)PPS (.*)>')
    # Capacity: Maxon 250mL
    CAPACITY = re.compile(r'Capacity:\s(Maxon|Pittman)\s+(\d+)mL')  # pump make and capacity
    # McLane Research Laboratories, Inc.
    # CF2 Adaptive Water Transfer System
    # Version 2.02  of Jun  7 2013 18:17
    #  Configured for: Maxon 250ml pump
    VERSION = re.compile(
        r'McLane .*$' + NEWLINE +
        r'CF2 .*$' + NEWLINE +
        r'Version\s+(\S+)\s+of\s+(.*)$' + NEWLINE +  # version and release date
        r'.*$'
    )


class DataParticleType(McLaneDataParticleType):
    """
    Data particle types produced by this driver
    """
    # TODO - define which commands will be published to user
    PPSDN_PARSED = 'ppsdn_parsed'


###############################################################################
# Data Particles
###############################################################################

# data particle for forward, reverse, and result commands
#  e.g.:
#                      --- command ---   -------- result -------------
#     Result port  |  vol flow minf tlim  |  vol flow minf secs date-time  |  batt code
#        Status 00 |  75 100  25   4 |   1.5  90.7  90.7*  1 031514 001727 | 29.9 0
class PPSDNSampleDataParticle(McLaneSampleDataParticle):
    _data_particle_type = DataParticleType.PPSDN_PARSED


###############################################################################
# Driver
###############################################################################

class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver subclass
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """

    def __init__(self, evt_callback):
        SingleConnectionInstrumentDriver.__init__(self, evt_callback)

    ########################################################################
    # Superclass overrides for resource query.
    ########################################################################

    @staticmethod
    def get_resource_params():
        """
        Return list of device parameters available.
        """
        return Parameter.list()

    ########################################################################
    # Protocol builder.
    ########################################################################

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(Prompt, NEWLINE, self._driver_event)


###########################################################################
# Protocol
###########################################################################

# noinspection PyMethodMayBeStatic,PyUnusedLocal
class Protocol(McLaneProtocol):
    """
    Instrument protocol class
    Subclasses CommandResponseInstrumentProtocol
    """

    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        # Construct protocol superclass.
        McLaneProtocol.__init__(self, prompts, newline, driver_event)

        # TODO - reset next_port on mechanical refresh of the PPS filters - how is the driver notified?
        # TODO - need to persist state for next_port to save driver restart
        self.next_port = 1  # next available port

        # get the following:
        # - VERSION
        # - CAPACITY (pump flow)
        # - BATTERY
        # - CODES (termination codes)
        # - COPYRIGHT (termination codes)

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        filling = self.get_current_state() == ProtocolState.FILL
        log.debug("_got_chunk:\n%s", chunk)
        sample_dict = self._extract_sample(PPSDNSampleDataParticle,
                                           PPSDNSampleDataParticle.regex_compiled(), chunk, timestamp, publish=filling)

        if sample_dict:
            self._linebuf = ''
            self._promptbuf = ''
            self._protocol_fsm.on_event(ProtocolEvent.PUMP_STATUS,
                                        PPSDNSampleDataParticle.regex_compiled().search(chunk))

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with XR-420 parameters.
        For each parameter key add value formatting function for set commands.
        """
        # Add parameter handlers to parameter dictionary for instrument configuration parameters.
        self._param_dict.add(Parameter.FLUSH_VOLUME,
                             r'Flush Volume: (.*)mL',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=FLUSH_VOLUME,
                             value=FLUSH_VOLUME,
                             units='mL',
                             startup_param=True,
                             display_name="flush_volume",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.FLUSH_FLOWRATE,
                             r'Flush Flow Rate: (.*)mL/min',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=FLUSH_RATE,
                             value=FLUSH_RATE,
                             units='mL/min',
                             startup_param=True,
                             display_name="flush_flow_rate",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.FLUSH_MINFLOW,
                             r'Flush Min Flow: (.*)mL/min',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=FLUSH_MIN_RATE,
                             value=FLUSH_MIN_RATE,
                             units='mL/min',
                             startup_param=True,
                             display_name="flush_min_flow",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.FILL_VOLUME,
                             r'Fill Volume: (.*)mL',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=FILL_VOLUME,
                             value=FILL_VOLUME,
                             units='mL',
                             startup_param=True,
                             display_name="fill_volume",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.FILL_FLOWRATE,
                             r'Fill Flow Rate: (.*)mL/min',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=FILL_RATE,
                             value=FILL_RATE,
                             units='mL/min',
                             startup_param=True,
                             display_name="fill_flow_rate",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.FILL_MINFLOW,
                             r'Fill Min Flow: (.*)mL/min',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=FILL_MIN_RATE,
                             value=FILL_MIN_RATE,
                             units='mL/min',
                             startup_param=True,
                             display_name="fill_min_flow",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.CLEAR_VOLUME,
                             r'Reverse Volume: (.*)mL',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=CLEAR_VOLUME,
                             value=CLEAR_VOLUME,
                             units='mL',
                             startup_param=True,
                             display_name="clear_volume",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.CLEAR_FLOWRATE,
                             r'Reverse Flow Rate: (.*)mL/min',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=CLEAR_RATE,
                             value=CLEAR_RATE,
                             units='mL/min',
                             startup_param=True,
                             display_name="clear_flow_rate",
                             visibility=ParameterDictVisibility.IMMUTABLE)
        self._param_dict.add(Parameter.CLEAR_MINFLOW,
                             r'Reverse Min Flow: (.*)mL/min',
                             None,
                             self._int_to_string,
                             type=ParameterDictType.INT,
                             default_value=CLEAR_MIN_RATE,
                             value=CLEAR_MIN_RATE,
                             units='mL/min',
                             startup_param=True,
                             display_name="clear_min_flow",
                             visibility=ParameterDictVisibility.IMMUTABLE)
