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
  inputs = []
  outputs = []
  
  def __init__(self, *args):
    thereWasIOSpec = self.__parseIOSpec(*args)
    if thereWasIOSpec:
      args = args[1:] #if the first element *was* an IO spec - remove it
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
  
  def __parseIOSpec(self, *args):
    #check if an input-output override was requested and handle it
    #expects input of following form:
    #   ([input1[,input2[...]]][->output1[,output2[...]]])
    #this means that "(item)" as well as "(->name,image)" are allowed
    #also, text between brackets must have spaces removed
    if len(args)==0 or not args[0].startswith('('):
      #no IO redirection spec - we're done
      return False  #not received anything
    if not args[0].endswith(')'):
      raise ParseIOSpecException(self.name, 'unbalanced brackets')
    io_spec = args[0][1:-1]
    if '->' in io_spec:
      io_in_, io_out_ = io_spec.split('->')
    else:
      io_in_  = io_spec
      io_out_ = ''
    #input override
    if io_in_ != '':
      io_in = io_in_.split(',')
      if len(io_in) != len(self.inputs):
        raise ParseIOSpecException(self.name, 'input spec length mismatch:' + 
              'expected {}, received {}'.format(len(self.inputs), len(io_in)))
      else:
        self.inputs = io_in
    #output override
    if io_out_ != '':
      io_out = io_out_.split(',')
      if len(io_out) != len(self.outputs):
        raise ParseIOSpecException(self.name, 'output spec length mismatch:' + 
              'expected {}, received {}'.format(len(self.outputs), len(io_out)))
      else:
        self.outputs = io_out
    return True #parsed inputs
  
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
  
  def getInputs(self):
    return self.inputs
  
  def getOutputs(self):
    return self.outputs
  
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

class ParseIOSpecException(muException):
  def __init__(self, taskname, message):
    self.message = message + ' (' + taskname + ')'
    super(ParseIOSpecException, self).__init__(self.message)
