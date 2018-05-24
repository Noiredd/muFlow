import unittest
import sys
sys.path.append('../muFlow')
import assembler as asm
import baseTasks as bt

class TestImport(unittest.TestCase):
  def test_import(self):
    a = asm.Assembler('../test/tasks')
    self.assertEqual(len(a.tasks.keys()), 1)
    self.assertIn('test', a.tasks.keys())

class TestAssembly(unittest.TestCase):
  a = asm.Assembler('../test/tasks')
  def test_instantiate(self):
    task = self.a.constructTask('test')
    self.assertIsInstance(task, bt.BaseProcessor)
  def test_instantiateFail(self):
    with self.assertRaises(asm.ConstructException):
      task = self.a.constructTask('noTask')
  def test_assembleText(self):
    flow = self.a.assembleFromText(['test'])
    self.assertIsInstance(flow, asm.FlowObject)
    self.assertEqual(len(flow.tasks), 1)
    self.assertEqual(flow.execute(None), 21)


if __name__=="__main__":
  unittest.main()
