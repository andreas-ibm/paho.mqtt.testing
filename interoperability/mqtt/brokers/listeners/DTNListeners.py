"""
*******************************************************************
  Copyright (c) 2013, 2018 IBM Corp.

  All rights reserved. This program and the accompanying materials
  are made available under the terms of the Eclipse Public License v1.0
  and Eclipse Distribution License v1.0 which accompany this distribution.

  The Eclipse Public License is available at
     http://www.eclipse.org/legal/epl-v10.html
  and the Eclipse Distribution License is available at
    http://www.eclipse.org/org/documents/edl-v10.php.

  Contributors:
     Ian Craggs - initial UDP version
     Andreas Martens - hacked up to read dtn from named pipe
*******************************************************************
"""

import socketserver, select, sys, traceback, socket, logging, getopt, hashlib, base64, json
import threading, ssl
import os, sys, subprocess

from mqtt.brokers.DTN import MQTTDTNBrokers
from mqtt.formats.MQTTSN import MQTTSNException

logger = logging.getLogger('MQTT broker')

def respond(handler, data):
  endpoint = handler.source
  send = subprocess.call(["dtnsend","--src","mqtt",endpoint],stdin=PIPE)
  send.stdin.communicate(input=data)
  send.stdin.close()

class DTNHandler():
  """
  This will take care of the messages that we receive and put them to the right data.
  """

  def handle(self, lines):
    logger.info("DTNListener: received: %s"%lines)
    messages = lines.split('\n')
    for line in messages:
      if not len(line) > 2:
        break
      message = json.loads(line)
      self.source = message['source']
      packet = base64.b64decode(message['payload'])
      # should pass down message.timestamp somehow...
      terminate = brokerDTN.handleRequest(packet, self.source, (respond, self))


class ThreadingDTNServer:
  pipe = ""
  handler = {}
  def __init__(self, pipename, handlerinstance):
    self.pipe = pipename
    try: os.mkfifo(self.pipe)
    except FileExistsError: pass
    self.handler = handlerinstance
    
  def read_forever(self):
    logger.debug("DTNListener: Starting read")
    while True:
      logger.debug("DTNListener: opening pipe")
      with open(self.pipe) as pipe_data:
        logger.debug("DTNListener: opened pipe")
        while True:
          logger.debug("DTNListener: reading from pipe")
          select.select([pipe_data],[],[pipe_data])
          line = pipe_data.read()
          if len(line) == 0:
            logger.debug("DTNListener: FIFO closed")
            break
          self.handler.handle(line)
    logger.debug("DTNListener: done")

  def shutdown(_):
    pass
        

def setBroker(aBrokerDTN):
  global brokerDTN
  brokerDTN = aBrokerDTN

def create(pipe="", serve_forever=False):
  logger.info("DTNListener: Starting DTN listener on pipe '%s'", pipe)
  server = ThreadingDTNServer(pipe, DTNHandler())
  thread = threading.Thread(target = server.read_forever)
  thread.daemon = True
  thread.start()
  return server

