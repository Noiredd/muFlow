import unittest
import sys
sys.path.append('../muFlow')
import baseTasks as bt

class TestTaskParams(unittest.TestCase):
  def test_noParams(self):
    class NoParamTask(bt.BaseProcessor):
      params = []
    NoParamTask.validateParams()
    task = NoParamTask()
    self.assertIsInstance(task, NoParamTask)
  def test_paramFormat(self):
    class BadParamTask(bt.BaseProcessor):
      params = [('param')]
    with self.assertRaises(bt.BadParamException):
      BadParamTask.validateParams()
  def test_paramName(self):
    class BadParamTask(bt.BaseProcessor):
      params = [('name',str)]
    with self.assertRaises(bt.BadParamException):
      BadParamTask.validateParams()
  def test_paramType(self):
    class BadParamTask(bt.BaseProcessor):
      params = [('param','str')]
    with self.assertRaises(bt.BadParamException):
      BadParamTask.validateParams()
  def test_success(self):
    class GoodParamTask(bt.BaseProcessor):
      params = [('param',str)]
    GoodParamTask.validateParams()
    task = GoodParamTask(params=['value'])
    self.assertIsInstance(task, GoodParamTask)
    a = task.param  #test that the param can be reached

class TestTaskInputs(unittest.TestCase):
  def test_defaultNoInput(self):
    class Task0(bt.BaseProcessor):
      def action(self):
        pass
    t0 = Task0()
    self.assertEqual(len(t0.inputs), 0)
  def test_defaultInput1(self):
    class Task1(bt.BaseProcessor):
      inputs = ['item']
      def action(self, item):
        pass
    t1 = Task1()
    self.assertEqual(len(t1.inputs), 1)
    self.assertEqual(t1.inputs[0], 'item')
  def test_defaultInput2(self):
    class Task2(bt.BaseProcessor):
      inputs = ['name', 'image']
      def action(self, name, image):
        pass
    t2 = Task2()
    self.assertEqual(len(t2.inputs), 2)
    self.assertEqual(t2.inputs[0], 'name')
    self.assertEqual(t2.inputs[1], 'image')

class TestTaskOutputs(unittest.TestCase):
  def test_defaultNoOutput(self):
    class Task0(bt.BaseProcessor):
      pass
      #makes sure that we *don't have to* specify an empty list
    t0 = Task0()
    self.assertEqual(len(t0.outputs), 0)
  def test_defaultOutput1(self):
    class Task1(bt.BaseProcessor):
      outputs = ['item']
    t1 = Task1()
    self.assertEqual(len(t1.outputs), 1)
    self.assertEqual(t1.outputs[0], 'item')
  def test_defaultOutput2(self):
    class Task2(bt.BaseProcessor):
      outputs = ['name', 'image']
    t2 = Task2()
    self.assertEqual(len(t2.outputs), 2)
    self.assertEqual(t2.outputs[0], 'name')
    self.assertEqual(t2.outputs[1], 'image')

class TestTaskInputOutput(unittest.TestCase):
  def test_one2one(self):
    class TaskOneToOne(bt.BaseProcessor):
      inputs = ['item']
      outputs = ['item']
      def action(self, item):
        return item
    task = TaskOneToOne()
    it_i = 5                  #Input
    it_e = it_i               #Expected output
    it_o = task.action(it_i)  #actual Output
    self.assertEqual(it_o, it_e)
  def test_one2many(self):
    class TaskOneToTwo(bt.BaseProcessor):
      inputs = ['item']
      outputs = ['item', 'thing']
      def action(self, item):
        return item, 2*item
    task = TaskOneToTwo()
    it_i = 5
    it_e = (5, 10)
    it_o = task.action(it_i)
    self.assertEqual(it_o, it_e)
  def test_many2one(self):
    class TaskTwoToOne(bt.BaseProcessor):
      inputs = ['item', 'thing']
      outputs = ['item']
      def action(self, item, thing):
        return item + thing
    task = TaskTwoToOne()
    it_i = (3, 2)
    it_e = 5
    it_o = task.action(*it_i)
    self.assertEqual(it_o, it_e)
  def test_many2many(self):
    class TaskTwoToThree(bt.BaseProcessor):
      inputs = ['item', 'thing']
      outputs = ['item', 'thing', 'stuff']
      def action(self, item, thing):
        return item+thing, item*thing, str(item)+str(thing)
    task = TaskTwoToThree()
    it_i = (3, 2)
    it_e = (5, 6, '32')
    it_o = task.action(*it_i)
    self.assertEqual(it_o, it_e)

class TestParams(unittest.TestCase):
  @classmethod
  def setUpClass(self):
    class SimpleTask(bt.BaseProcessor):
      params = [('arg',str)]
    self.SimpleTask = SimpleTask
    self.SimpleTask.validateParams()
    class ParamsTask(bt.BaseProcessor):
      params = [('count',int), ('number',float), ('text',str)]
    self.ParamsTask = ParamsTask
    self.ParamsTask.validateParams()
    class DefaultParamTask(bt.BaseProcessor):
      params = [('count', int, 1)]
    self.DefaultParamTask = DefaultParamTask
    self.DefaultParamTask.validateParams()
    class DefaultParamsTask(bt.BaseProcessor):
      params = [('a', int), ('b', int), ('c', int, 1)]
    self.DefaultParamsTask = DefaultParamsTask
    self.DefaultParamsTask.validateParams()
    class IOTaskInput(bt.BaseProcessor):
      inputs = ['item', 'name']
    self.IOTaskInput = IOTaskInput
    self.IOTaskInput.validateParams()
    class IOTaskOutput(bt.BaseProcessor):
      outputs = ['image', 'mask', 'alpha']
    self.IOTaskOutput = IOTaskOutput
    self.IOTaskOutput.validateParams()
    class IOTaskInOut(bt.BaseProcessor):
      inputs = ['in1', 'in2']
      outputs = ['out']
    self.IOTaskInOut = IOTaskInOut
    self.IOTaskInOut.validateParams()
    class IOTaskInPlace(bt.BaseProcessor):
      inputs = ['image']
      outputs = ['image']
    self.IOTaskInPlace = IOTaskInPlace
    self.IOTaskInPlace.validateParams()
    class EnumParamTask(bt.BaseProcessor):
      modes = {'A': 1, 'B': 2, 'C': 4}
      params = [('mode', modes, 'A')]
    self.EnumParamTask = EnumParamTask
    self.EnumParamTask.validateParams()
  #parameter counting
  def test_notEnoughParams(self):
    with self.assertRaises(bt.ConstructException):
      task = self.SimpleTask()
  def test_tooManyParams(self):
    with self.assertRaises(bt.ConstructException):
      task = self.SimpleTask(params=['value1', 'value2'])
  def test_memberCreate(self):
    task = self.SimpleTask(params=['value'])
    self.assertTrue(hasattr(task, 'arg'))
  #parameter parsing
  def test_parseStr(self):
    task = self.ParamsTask(params=['5', '2.0', 'value'])
    self.assertEqual(type(task.text), str)
    self.assertEqual(task.text, 'value')
  def test_parseInt(self):
    task = self.ParamsTask(params=['5', '2.0', 'value'])
    self.assertEqual(type(task.count), int)
    self.assertEqual(task.count, 5)
  def test_parseFloat(self):
    task = self.ParamsTask(params=['5', '2.2', 'value'])
    self.assertEqual(type(task.number), float)
    self.assertEqual(task.number, 2.2)
  def test_parseIntFail(self):
    with self.assertRaises(bt.ConstructException):
      task = self.ParamsTask(params=['5x', '2.2', 'value'])
  def test_parseFloatFail(self):
    with self.assertRaises(bt.ConstructException):
      task = self.ParamsTask(params=['5', 'er2.2', 'value'])
  def test_parseDefaultParam(self):
    task = self.DefaultParamTask()
    self.assertEqual(type(task.count), int)
    self.assertEqual(task.count, 1)
    task = self.DefaultParamTask(params=['2'])
    self.assertEqual(task.count, 2)
  def test_parseDefaultParams(self):
    task = self.DefaultParamsTask(params=['1', '2', '3'])
    self.assertEqual(task.a, 1)
    self.assertEqual(task.b, 2)
    self.assertEqual(task.c, 3)
    task = self.DefaultParamsTask(params=['1', '2'])
    self.assertEqual(task.a, 1)
    self.assertEqual(task.b, 2)
    self.assertEqual(task.c, 1)
  #input/output override
  def test_parseInputNumFail(self):
    with self.assertRaises(bt.ConstructException):
      task = self.IOTaskInput(args=['thing'])
  def test_parseInputOverride(self):
    task = self.IOTaskInput(args=['thing', 'filename'])
    expected_inputs = ['thing', 'filename']
    self.assertEqual(expected_inputs, task.getInputs())
  def test_parseOutputOverride(self):
    task = self.IOTaskOutput(dest=['rgb', 'ignore', 'a'])
    expected_outputs = ['rgb', 'ignore', 'a']
    self.assertEqual(expected_outputs, task.getOutputs())
  def test_parseInputOutput(self):
    task = self.IOTaskInOut(args=['a', 'b'], dest=['c'])
    expected_inputs = ['a', 'b']
    expected_outputs = ['c']
    self.assertEqual(expected_inputs, task.getInputs())
    self.assertEqual(expected_outputs, task.getOutputs())
  def test_parseInPlace(self):
    task = self.IOTaskInPlace(args=['items'])
    expected_inputs = ['items']
    expected_outputs = ['items']
    self.assertEqual(expected_inputs, task.getInputs())
    self.assertEqual(expected_outputs, task.getOutputs())
  #enumeration type params
  def test_parseEnum(self):
    task = self.EnumParamTask(params=['B'])
    self.assertEqual(task.mode, 2)
  def test_parseEnumFail(self):
    with self.assertRaises(bt.UserException):
      task = self.EnumParamTask(params=['D'])
  def test_parseEnumDefault(self):
    task = self.EnumParamTask()
    self.assertEqual(task.mode, 1)

class TestReduction(unittest.TestCase):
  @classmethod
  def setUpClass(self):
    class TestReducer(bt.BaseReducer):
      name = 'simple'
      inputs = ['item']
      outputs = ['item']
      def reduction(self, a, b):
        return a + b
    self.TestReducer = TestReducer
    self.TestReducer.validateParams()
  def test_rename(self):
    self.assertEqual(self.TestReducer.name, 'reduce_simple')
  #input size validation
  def test_defaultIO(self):
    class TestReducer(bt.BaseReducer):
      name = 'correct'
    task = TestReducer()
  def test_notEnoughInputs(self):
    class TestReducer(bt.BaseReducer):
      name = 'fail'
      inputs = []
    with self.assertRaises(bt.UserException):
      task = TestReducer()
  def test_tooManyOutputs(self):
    class TestReducer(bt.BaseReducer):
      name = 'fail'
      outputs = ['item', 'another']
    with self.assertRaises(bt.UserException):
      task = TestReducer()
  #basic reduction operation (buffering, returning)
  def test_basicReduction(self):
    task = self.TestReducer()
    data = [3, 5, 8, 0, 2]
    sumd = [sum(data)]
    for d in data:
      task.action(d)
    self.assertEqual(task.output(), sumd)

if __name__=="__main__":
  print("Running base task tests...")
  unittest.main()
