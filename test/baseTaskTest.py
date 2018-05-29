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
    task = GoodParamTask('value')
    self.assertIsInstance(task, GoodParamTask)
    a = task.param  #test that the param can be reached

class TestParsing(unittest.TestCase):
  class SimpleTask(bt.BaseProcessor):
    params = [('arg',str)]
  class ParamsTask(bt.BaseProcessor):
    params = [('count',int), ('number',float), ('text',str)]
  #argument counting
  def test_notEnoughArgs(self):
    with self.assertRaises(bt.ArgsListException):
      task = self.SimpleTask()
  def test_tooManyArgs(self):
    with self.assertRaises(bt.ArgsListException):
      task = self.SimpleTask('value1', 'value2')
  def test_memberCreate(self):
    task = self.SimpleTask('value')
    self.assertTrue(hasattr(task, 'arg'))
  #argument parsing
  def test_parseStr(self):
    task = self.ParamsTask('5', '2.0', 'value')
    self.assertEqual(type(task.text), str)
    self.assertEqual(task.text, 'value')
  def test_parseInt(self):
    task = self.ParamsTask('5', '2.0', 'value')
    self.assertEqual(type(task.count), int)
    self.assertEqual(task.count, 5)
  def test_parseFloat(self):
    task = self.ParamsTask('5', '2.2', 'value')
    self.assertEqual(type(task.number), float)
    self.assertEqual(task.number, 2.2)
  def test_parseIntFail(self):
    with self.assertRaises(bt.ParseArgsException):
      task = self.ParamsTask('5x', '2.2', 'value')
  def test_parseFloatFail(self):
    with self.assertRaises(bt.ParseArgsException):
      task = self.ParamsTask('5', 'er2.2', 'value')


if __name__=="__main__":
  unittest.main()
