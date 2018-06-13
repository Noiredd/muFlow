import unittest
import sys
sys.path.append('../muFlow')
import assembler as asm
import baseTasks as bt

class TestImport(unittest.TestCase):
  def test_import(self):
    a = asm.Assembler('../test/tasks')
    self.assertEqual(len(a.tasks.keys()), 3)
    self.assertIn('test', a.tasks.keys())
    self.assertIn('add', a.tasks.keys())
    self.assertIn('dup', a.tasks.keys())

class TestAssemblerBasics(unittest.TestCase):
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
    self.assertEqual(flow.execute()['item'], 2)
  def test_assembleMulti(self):
    flow = self.a.assembleFromText(['test', 'add 1', 'add 2'])
    self.assertEqual(flow.execute()['item'], 5)
  def test_assembleThreeway(self):
    flow = self.a.assembleFromText(['test', 'dup', 'add 3'])
    rslt = flow.execute()
    self.assertEqual(rslt['item'], 5)
    self.assertEqual(rslt['thing'], 2)
  
  class TestAssemblerAdvanced(unittest.TestCase):
    a = asm.Assembler('../test/tasks')
    def testCustomFlow(self):
      text = ['test (->val)', 'dup (val->val1,val2)', 'add (val1->val1) 3', 'add (val2->val2) 1']
      flow = self.a.assembleFromText(text)
      rslt = flow.execute()
      self.assertEqual(rslt['val1'], 5)
      self.assertEqual(rslt['val2'], 3)
    def testCustomFlowSpaces(self):
      text = ['test(->val)', 'dup (val -> val1, val2)', 'add (val1 -> val1) 3', 'add(val2->val2)1']
      flow = self.a.assembleFromText(text)
      rslt = flow.execute()
      self.assertEqual(rslt['val1'], 5)
      self.assertEqual(rslt['val2'], 3)
    def testCustomFlowFailScope(self):
      text = ['test (->val)', 'add (val1->val1) 3',]
      with self.assertRaises(bt.ParseIOSpecException):
        flow = self.a.assembleFromText(text)
    def testCustomFlowFailBrackets(self):
      text = ['test (->val', 'add (val->val) 3',]
      with self.assertRaises(bt.ParseIOSpecException):
        flow = self.a.assembleFromText(text)

if __name__=="__main__":
  unittest.main()
