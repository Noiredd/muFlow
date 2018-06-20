import unittest
import sys
sys.path.append('../muFlow')
import assembler as asm
import baseTasks as bt

class TestImport(unittest.TestCase):
  assembler = asm.Assembler('../test/tasks')
  def test_importSerial(self):
    a = self.assembler
    self.assertEqual(len(a.tasks_serial.keys()), 5)
    self.assertIn('src', a.tasks_serial.keys())
    self.assertIn('add', a.tasks_serial.keys())
    self.assertIn('dup', a.tasks_serial.keys())
    self.assertIn('lst', a.tasks_serial.keys())
    self.assertIn('get', a.tasks_serial.keys())
  def test_importParallel(self):
    a = self.assembler
    self.assertEqual(len(a.tasks_parallel.keys()), 3)
    self.assertIn('incr', a.tasks_parallel.keys())
    self.assertIn('vmul', a.tasks_parallel.keys())
    self.assertIn('simo', a.tasks_parallel.keys())

class TestAssemblerBasics(unittest.TestCase):
  a = asm.Assembler('../test/tasks')
  def test_instantiate(self):
    _, task = self.a.constructTask('src 5')
    self.assertIsInstance(task, bt.BaseProcessor)
  def test_instantiateFail(self):
    with self.assertRaises(asm.ConstructException):
      _, task = self.a.constructTask('noTask')
  def test_setupTask(self):
    flow = self.a.assembleFromText(['src 2', 'add -1'])
    flow.execute()
    self.assertTrue(flow.tasks[1].ready)
  def test_assembleText(self):
    flow = self.a.assembleFromText(['src 2'])
    self.assertIsInstance(flow, asm.MacroFlow)
    self.assertEqual(len(flow.tasks), 1)
    self.assertEqual(flow.execute()['item'], 2)
  def test_assembleMulti(self):
    flow = self.a.assembleFromText(['src 2', 'add 1', 'add 2'])
    self.assertEqual(flow.execute()['item'], 5)
  def test_assembleThreeway(self):
    flow = self.a.assembleFromText(['src 2', 'dup', 'add 3'])
    rslt = flow.execute()
    self.assertEqual(rslt['item'], 5)
    self.assertEqual(rslt['thing'], 2)
  
class TestAssemblerAdvanced(unittest.TestCase):
  a = asm.Assembler('../test/tasks')
  def test_customFlow(self):
    text = ['src (->val) 0', 'dup (val->val1,val2)', 'add (val1->val1) 3', 'add (val2->val2) 1']
    flow = self.a.assembleFromText(text)
    rslt = flow.execute()
    self.assertEqual(rslt['val1'], 3)
    self.assertEqual(rslt['val2'], 1)
  def test_customFlowSpaces(self):
    text = ['src(->val) -1', 'dup (val -> val1, val2)', 'add (val1 -> val1) 3', 'add(val2->val2)1']
    flow = self.a.assembleFromText(text)
    rslt = flow.execute()
    self.assertEqual(rslt['val1'], 2)
    self.assertEqual(rslt['val2'], 0)
  def test_customFlowFailInputs(self):
    text = ['src 0', 'dup (item, item->item, val)']
    with self.assertRaises(bt.ParseIOSpecException):
      flow = self.a.assembleFromText(text)
  def test_customFlowFailScope(self):
    text = ['src (->val) -4', 'add (val1->val1) 3']
    with self.assertRaises(asm.ConstructException):
      flow = self.a.assembleFromText(text)
  def test_customFlowFailBrackets(self):
    text = ['src (->val 3', 'add (val->val) 3']
    with self.assertRaises(asm.ConstructException):
      flow = self.a.assembleFromText(text)

class TestMicroFlow(unittest.TestCase):
  #Parallel tasks cannot be tested in baseTaskTest as they rely on MicroFlow
  def test_parallelSetup(self):
    class TaskParallelSetupTest(bt.BaseParallel):
      inputs = ['items']
      outputs = ['items']
      val = 1
      def setup(self, **kwargs):
        self.val += 1
      def action(self, x):
        return x * self.val
    pTask = TaskParallelSetupTest()
    test_data = [1, 2, 1, 3, 1]
    expected  = [2, 4, 2, 6, 2]
    scope = {'items': test_data}
    uFlow = asm.MicroFlow(scope)
    uFlow.append(pTask)
    uFlow.gather('items')
    uFlow.setup()
    result = uFlow.action(scope['items'])
    self.assertEqual(result, expected)
  def test_parallelSimple(self):
    class TaskParallelSimple(bt.BaseParallel):
      inputs = ['items']
      outputs = ['items']
      def action(self, x):
        return x + 1
    pTask = TaskParallelSimple()
    test_data = [1, 2, 4, 6, 9]
    expected  = [2, 3, 5, 7, 10]
    scope = {'items': test_data}
    uFlow = asm.MicroFlow(scope)
    uFlow.append(pTask)
    uFlow.gather('items')
    uFlow.setup()
    result = uFlow.action(scope['items'])
    self.assertEqual(result, expected)
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
      outputs = ['items']
      def action(self, x, y):
        return x * y
    test_data = [1, 2, 4, 6]
    expected  = [0, 3, 15, 35]
    scope = {'items': test_data}
    uFlow = asm.MicroFlow(scope)
    uFlow.append(TaskIncrement())
    uFlow.append(TaskDecrement())
    uFlow.append(TaskMultiply())
    uFlow.gather('items')
    uFlow.setup()
    result = uFlow.action(scope['items'])
    self.assertEqual(result, expected)

class TestAssemblerParallel(unittest.TestCase):
  a = asm.Assembler('../test/tasks')
  def test_parallelFlow(self):
    expected = [1, 2, 3, 4, 5]
    text = ['lst 5', 'incr (item->plus1) 1', 'dup(plus1->a,b)']
    flow = self.a.assembleFromText(text)
    flow.execute()
    self.assertIn('plus1', flow.scope.keys())
    self.assertEqual(flow.scope['plus1'], expected)
  def test_multipleOutputsFlow(self):
    expected_c = [0.5, 1.0, 1.5, 2.0]
    expected_d = [2, 4, 6, 8]
    text = ['lst (->a) 4', 'incr (a->b) 1', 'simo (b->c,d) 2', 'get(c)', 'get(d)']
    flow = self.a.assembleFromText(text)
    flow.execute()
    self.assertIn('c', flow.scope.keys())
    self.assertIn('d', flow.scope.keys())
    self.assertEqual(flow.scope['c'], expected_c)
    self.assertEqual(flow.scope['d'], expected_d)
  def test_multipleFlows(self):
    expected_b = [1, 2, 3, 4]
    expected_d = [2, 6, 12, 20]
    text = ['lst (->a) 4', 'incr (a->b) 1', 'get (b)', 'incr (a->c) 2', 'vmul (b,c->d)','get (d)']
    flow = self.a.assembleFromText(text)
    flow.execute()
    self.assertIn('b', flow.scope.keys())
    self.assertIn('d', flow.scope.keys())
    self.assertNotIn('c', flow.scope.keys())
    self.assertEqual(flow.scope['b'], expected_b)
    self.assertEqual(flow.scope['d'], expected_d)

if __name__=="__main__":
  print("Running assembler tests...")
  unittest.main()
