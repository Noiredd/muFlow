#####Python 2 and 3 compatibility#####
import sys
if sys.version_info < (3,0):
  from exceptions import Exception
#####Python 2 and 3 compatibility#####

class BaseProcessor(object):
  #Basic factory for constructing flow object for given tasks
  name = ''
  info = ''
  params = []
  inputs = []
  outputs = []
  isValid = False
  
  def __init__(self, *args):
    #if the task has not been yet validated - now is the time
    if not self.isValid:
      self.validateParams()
    #parse the IO specification, if present, and remove it from the args list
    thereWasIOSpec = self.__parseIOSpec(*args)
    if thereWasIOSpec:
      args = args[1:]
    self.__parseArgs(*args)
  
  @classmethod
  def validateParams(cls):
    #make sure params have a proper layout:
    #(name, type[, default value])  - def val only allowed at end
    cls.requiredArgs = 0
    cls.maximumArgs  = 0
    def itsRequired(cls):
      cls.requiredArgs += 1
      cls.maximumArgs  += 1
    def itsDefault(cls):
      cls.maximumArgs  += 1
    onDefault = False
    for param in cls.params:
      l = len(param)
      if l not in (2, 3):
        raise BadParamException(cls.name, param, 'is incorrect format')
      if not onDefault:
        if l == 3:
          onDefault = True
          itsDefault(cls)
        else:
          itsRequired(cls)
      else:
        itsDefault(cls)
        if l < 3:
          raise BadParamException(cls.name, param, 'misses default value')
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
    #make sure defaults are convertible to given types
    for param in cls.params:
      if len(param) == 3:
        try:
          param[1]( param[2] )
        except TypeError:
          raise BadParamException(cls.name, param, 'cannot convert default value')
        except ValueError:
          raise BadParamException(cls.name, param, 'bad default value')
    #nothing needs to be returned - if this completes, we're good
    cls.isValid = True
  
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
        raise ParseIOSpecException(self.name, 'input spec length mismatch: ' +
              'expected {}, received {}'.format(len(self.inputs), len(io_in)))
      else:
        #for tasks that work "in-place", change the output as well
        if self.inputs == self.outputs:
          self.outputs = io_in
        self.inputs = io_in
    #output override
    if io_out_ != '':
      io_out = io_out_.split(',')
      if len(io_out) != len(self.outputs):
        raise ParseIOSpecException(self.name, 'output spec length mismatch: ' +
              'expected {}, received {}'.format(len(self.outputs), len(io_out)))
      else:
        self.outputs = io_out
    return True #parsed inputs
  
  def __parseArgs(self, *args):
    #ensure the right number of arguments, optionally fill with defaults
    defs = ()
    if len(args) < self.requiredArgs:
      raise ArgsListException(self.name, 'not enough arguments')
    else:
      if len(args) > self.maximumArgs:
        raise ArgsListException(self.name, 'too many arguments')
      elif len(args) < self.maximumArgs:
        defaults = [p[2] for p in self.params if len(p)>2]
        overlap = len(defaults) - self.maximumArgs + len(args)
        defs = tuple(defaults[overlap:])
    #parse and assign to the object
    for arg, param in zip(args+defs, self.params):
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

class BaseParallel(BaseProcessor):
  def __init__(self, *args):
    super(BaseParallel, self).__init__(*args)

class BaseReducer(BaseProcessor):
  #This task behaves as both serial and parallel task. When added to the Flow,
  #it is first constructed as a parallel task, allowing fast reduction of the
  #input in subprocesses, each returning a product of reduction of its sublist
  #packed as a list (of 1 element). Then the task is instantiated again, as a
  #serial task, performing the same reduction of the list of reduced sublists.
  inputs = ['item']
  outputs = ['item']

  @classmethod
  def validateParams(cls):
    if len(cls.inputs) != 1 or len(cls.outputs) != 1:
      raise UserException(cls.name, 'reducers can only I/O a single item')
    cls.name = 'reduce_' + cls.name
    super(BaseReducer, cls).validateParams()

  def __init__(self, *args):
    super(BaseReducer, self).__init__(*args)
    self.action = self.action_first

  def output(self):
    return self.final(self.accumulator)

  def action_first(self, item):
    #initializes the accumulator and changes action into the proper function
    self.accumulator = item
    self.action = self.action_every

  def action_every(self, item):
    #reduces every next item into the accumulator
    self.accumulator = self.reduction(item, self.accumulator)

  def action_final(self, item):
    #wrapper for executing from MacroFlow
    self.accumulator = item[0]
    for i in item[1:]:
      self.action_every(i)
    return self.final(self.accumulator)

  def setup(self, **kwargs):
    #non-parallelized user code, ran once
    #set all your counters etc. here
    pass

  def reduction(self, a, b):
    #define how to reduce two elements here
    return a

  def final(self, item):
    #this will be called on the reduced output right before returning it
    return item

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
