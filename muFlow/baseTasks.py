from errors import *

class BaseProcessor(object):
  #Base class for all tasks. Its class method validates user-defined tasks,
  #its constructor instantiates a task object, parses arguments etc.
  #User tasks shall derive either directly from this class (in this case
  #they become Serial tasks) or from one of its base descendants (for
  #Parallel or Reducer tasks - see below). Serial tasks can in theory
  #input and output any type of objects, but care should be taken that
  #parallel tasks can only operate on objects aggregated in lists. For
  #this reason, if a result of a Serial task will be further processed
  #by some Parallel task, this result has to either be a list, or be
  #wrapped in a list.
  name = ''
  info = ''
  params = []
  inputs = []
  outputs = []
  isValid = False

  def __init__(self, args=[], dest=[], params=[], **kwargs):
    #print(self.name, args, dest, params)
    #if the task has not been yet validated - now is the time
    if not self.isValid:
      self.validateParams()
    #check the input/output override
    if len(args) > 0:
      if len(args) != len(self.inputs):
        raise ConstructException(self.name, 'input length mismatch: ' +
              'expected {}, received {}'.format(len(self.inputs), len(args)))
      #for tasks that work "in-place", change the output as well
      if self.inputs == self.outputs:
        self.outputs = args
      self.inputs = args
    if len(dest):
      if len(dest) != len(self.outputs):
        raise ConstructException(self.name, 'output length mismatch: ' +
              'expected {}, received {}'.format(len(self.outputs), len(dest)))
      self.outputs = dest
    #check the parameters
    if len(params) < self.requiredArgs:
      raise ConstructException(self.name, 'not enough arguments')
    if len(params) > self.maximumArgs:
      raise ConstructException(self.name, 'too many arguments')
    #assign parameter values, optionally filling in with defaults
    for i, param in enumerate(self.params):
      param_name = param[0]
      param_type = param[1]
      if i < len(params):
        param_value = params[i]
      else:
        param_value = param[2]
      try:
        typed_value = param_type(param_value)
      except ValueError:
        raise ConstructException(self.name,
              'bad type for param "{}" - expected a {}, received "{}"'.format(
                param_name, str(param_type).split("'")[1], str(param_value)))
      setattr(self, param_name, typed_value)

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
  #Parallel tasks can only input lists. With multiple inputs, all lists must
  #be the same length.
  def __init__(self, args=[], dest=[], params=[], **kwargs):
    super(BaseParallel, self).__init__(args, dest, params, **kwargs)

class BaseReducer(BaseProcessor):
  #This task behaves as both serial and parallel task. When added to the Flow,
  #it is first constructed as a parallel task, allowing fast reduction of the
  #input in subprocesses, each returning a product of reduction of its sublist
  #packed as a list (of 1 element). Then the task is instantiated again, as a
  #serial task, performing the same reduction of the list of reduced sublists.
  #Reducer tasks input lists of multiple elements and return a list with only
  #a single element.
  inputs = ['item']
  outputs = ['item']

  @classmethod
  def validateParams(cls):
    if len(cls.inputs) != 1 or len(cls.outputs) != 1:
      raise UserException(cls.name, 'reducers can only I/O a single item')
    if not cls.name.startswith('reduce_'):
      cls.name = 'reduce_' + cls.name
    super(BaseReducer, cls).validateParams()

  def __init__(self, args=[], dest=[], params=[], **kwargs):
    super(BaseReducer, self).__init__(args, dest, params, **kwargs)
    self.action = self.action_first

  def output(self):
    return [self.final(self.accumulator)]

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
    return self.output()

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
