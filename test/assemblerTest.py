import unittest
import sys
sys.path.append('../muFlow')
import assembler as asm
import baseTasks as bt

class TestImport(unittest.TestCase):
  assembler = asm.Assembler('../test/tasks')
  def test_importSerial(self):
    a = self.assembler
    self.assertEqual(len(a.tasks_serial.keys()), 4)
    self.assertIn('test', a.tasks_serial.keys())
    self.assertIn('add', a.tasks_serial.keys())
    self.assertIn('dup', a.tasks_serial.keys())
    self.assertIn('list', a.tasks_serial.keys())
  def test_importParallel(self):
    a = self.assembler
    self.assertEqual(len(a.tasks_parallel.keys()), 3)
    self.assertIn('inc_each', a.tasks_parallel.keys())

class TestAssemblerBasics(unittest.TestCase):
  a = asm.Assembler('../test/tasks')
  def test_instantiate(self):
    _, task = self.a.constructTask('test')
    self.assertIsInstance(task, bt.BaseProcessor)
  def test_instantiateFail(self):
    with self.assertRaises(asm.ConstructException):
      _, task = self.a.constructTask('noTask')
  def test_setupTask(self):
    flow = self.a.assembleFromText(['test', 'add -1'])
    flow.execute()
    self.assertTrue(flow.tasks[1].ready)
  def test_assembleText(self):
    flow = self.a.assembleFromText(['test'])
    self.assertIsInstance(flow, asm.MacroFlow)
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
  def testCustomFlowFailInputs(self):
    text = ['test', 'dup (item, item->item, val)']
    with self.assertRaises(bt.ParseIOSpecException):
      flow = self.a.assembleFromText(text)
  def testCustomFlowFailScope(self):
    text = ['test (->val)', 'add (val1->val1) 3']
    with self.assertRaises(asm.ConstructException):
      flow = self.a.assembleFromText(text)
  def testCustomFlowFailBrackets(self):
    text = ['test (->val', 'add (val->val) 3']
    with self.assertRaises(asm.ConstructException):
      flow = self.a.assembleFromText(text)

class TestMicroFlow(unittest.TestCase):
  #Parallel tasks cannot be tested in baseTaskTest as they rely on MicroFlow
  def test_parallelSimple(self):
    class TaskParallelSimple(bt.BaseParallel):
      inputs = ['items']
      outputs = ['items']
      capture = []
      def action(self, x):
        self.capture.append(x + 1)
    pTask = TaskParallelSimple()
    test_data = [1, 2, 4, 6, 9]
    expected  = [2, 3, 5, 7, 10]
    scope = {'items': test_data}
    uFlow = asm.MicroFlow(scope)
    uFlow.append(pTask)
    uFlow.action(scope['items'])
    self.assertEqual(pTask.capture, expected)
  def test_parallelFlow(self):
    class TaskIncrement(bt.BaseParallel):
      inputs = ['items']
      outputs = ['incremented']
      def action(self, x):
        return x + 1
    class TaskDecrement(bt.BaseParallel):
      inputs = ['items']
      outputs = ['decremented']
      def action(self, x):
        return x - 1
    class TaskMultiply(bt.BaseParallel):
      inputs = ['incremented', 'decremented']
      capture = []
      def action(self, x, y):
        self.capture.append(x * y)
    test_data = [1, 2, 4, 6]
    expected  = [0, 3, 15, 35]
    scope = {'items': test_data}
    uFlow = asm.MicroFlow(scope)
    uFlow.append(TaskIncrement())
    uFlow.append(TaskDecrement())
    mTask = TaskMultiply()
    uFlow.append(mTask)
    uFlow.action(scope['items'])
    self.assertEqual(mTask.capture, expected)

if __name__=="__main__":
  unittest.main()
