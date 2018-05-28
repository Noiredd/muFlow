#####Python 2 and 3 compatibility#####
import sys
if sys.version_info < (3,0):
  from exceptions import Exception
muException = Exception
#####Python 2 and 3 compatibility#####

class BaseProcessor(object):
  #Basic factory for constructing flow object for given tasks
  name = ''
  info = ''
  params = []
  
  def __init__(self, *args):
    self.__parseArgs(*args)
  
  @classmethod
  def validateParams(cls):
    #make sure params have 2 elements
    for param in cls.params:
      if len(param) != 2:
        raise BadParamException(cls.name, param, 'is incorrect format')
    #make sure the class itself uses no forbidden words in its params
    #(otherwise, parsing args might result in overwriting some members)
    forbidden = BaseProcessor.__dict__.keys()
    for param in cls.params:
      if param[0] in forbidden:
        raise BadParamException(cls.name, param, 'overrides a class member')
    #make sure the params' 1-nd elements are callable on a string
    for param in cls.params:
      try:
        param[1]('test')
      except TypeError:
        raise BadParamException(cls.name, param, 'is not callable')
      except ValueError:
        #don't care about being unable to actually convert the string
        pass
    #nothing needs to be returned - if this completes, we're good
  
  def __parseArgs(self, *args):
    #make sure we get the right number of arguments
    required_param_count = len(self.params)
    if len(args) < required_param_count:
      raise ArgsListException(self.name, 'not enough arguments')
    elif len(args) > required_param_count:
      raise ArgsListException(self.name, 'too many arguments')
    #parse and assign to the object
    for arg, param in zip(args, self.params):
      arg_name = param[0]
      arg_type = param[1]
      try:
        c_arg = arg_type(arg)
      except ValueError:
        raise ParseArgsException(self.name, param)
      setattr(self, arg_name, c_arg)
  
  def setup(self, **kwargs):
    #non-parallelized user code
    pass
  
  def action(self, *args):
    #parallelized (per-item) user code
    pass


class BadParamException(muException):
  def __init__(self, taskname, param, text):
    self.message = param[0] + ' ' + text + ' (' + taskname + ')'
    super(BadParamException, self).__init__(self.message)

class ArgsListException(muException):
  def __init__(self, taskname, text):
    self.message = text + ' (' + taskname + ')'
    super(ArgsListException, self).__init__(self.message)

class ParseArgsException(muException):
  def __init__(self, taskname, param):
    def s(t):
      return str(t).split("'")[1]
    self.message = 'bad type for param "' + param[0] + '" - expected a ' + s(param[1]) + ' (' + taskname + ')'
    super(ParseArgsException, self).__init__(self.message)

