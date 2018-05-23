import unittest
import sys
sys.path.append('../muFlow')
import assembler as asm

class TestImport(unittest.TestCase):
  def test_import(self):
    a = asm.Assembler('../test/tasks')
    self.assertEqual(len(a.tasks.keys()), 1)
    self.assertTrue('test' in a.tasks.keys())


if __name__=="__main__":
  unittest.main()
