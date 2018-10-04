#####Python 2 and 3 compatibility#####
import sys
if sys.version_info < (3,0):
  from exceptions import Exception

class muException(Exception):
  def __init__(self, message):
    super(muException, self).__init__(self.message)
  def die(self):
    print('{}: {}'.format(self.__class__.__name__, self.message))
    exit()

class UserException(muException):
  def __init__(self, taskname, message):
    self.message = '[{}]: {}'.format(taskname, message)
    super(UserException, self).__init__(self.message)

class BadParamException(muException):
  def __init__(self, taskname, param, text):
    self.message = param[0] + ' ' + text + ' (' + taskname + ')'
    super(BadParamException, self).__init__(self.message)

class ConstructException(muException):
  def __init__(self, taskname, text):
    self.message = text + ' (' + taskname + ')'
    super(ConstructException, self).__init__(self.message)

class ParsingException(muException):
  def __init__(self, token, state, line=''):
    self.message = 'Unexpected {} when scanning for {}.'.format(token.debug, state.value)
    if line == '':
      self.error = 'Source line not available.'
    else:
      f = '{:>' + str(token.pos + 1) + '}'
      self.error = line + '\n' + f.format('^')
    super(ParsingException, self).__init__(self.message)
  def die(self):
    print("ParsingException: " + self.message)
    print(self.error)
    exit()
