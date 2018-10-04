import unittest
import sys
sys.path.append('../muFlow')
import muparse

class TestParser(unittest.TestCase):
  @classmethod
  def setUpClass(self):
    self.parser = muparse.Parser()
  def makeDict(self, name='', args=[], dest=[], params=[]):
    return {
      'name': name,
      'args': [args] if type(args) is str else args,
      'dest': [dest] if type(dest) is str else dest,
      'params': [params] if type(params) is str else params
    }
  def parseTest(self, text, expect):
    result = self.parser.parseText(text)
    self.assertEqual(result.keys(), expect.keys())
    for key in result.keys():
      r = result[key]
      e = expect[key]
      self.assertEqual(r, e, msg="Failed to parse '{}': '{}' != '{}'.".format(key, r, e))
  def test_empty(self):
    text = ''
    expect = self.makeDict()
    self.parseTest(text, expect)
  def test_bareName(self):
    text = 'task'
    expect = self.makeDict(name='task')
    self.parseTest(text, expect)
  def test_param1(self):
    text = 'task param'
    expect = self.makeDict(name='task', params='param')
    self.parseTest(text, expect)
  def test_paramN(self):
    text = 'task param1 param2 param3'
    expect = self.makeDict(name='task', params=['param1', 'param2', 'param3'])
    self.parseTest(text, expect)
  def test_comment(self):
    text = 'task param1 #param2'
    expect = self.makeDict(name='task', params=['param1'])
    self.parseTest(text, expect)
  def test_paramQuoted(self):
    text = 'task "param 1" param2 "p#ram 3"'
    expect = self.makeDict(name='task', params=['param 1', 'param2', 'p#ram 3'])
    self.parseTest(text, expect)
  def test_IOspecEmpty(self):
    text = 'task ()'
    expect = self.makeDict(name='task')
    self.parseTest(text, expect)
  def test_IOspecArg1(self):
    text = 'task(arg)'
    expect = self.makeDict(name='task', args=['arg'])
    self.parseTest(text, expect)
  def test_IOspecArgN(self):
    text = 'task (arg1 arg2)'
    expect = self.makeDict(name='task', args=['arg1', 'arg2'])
    self.parseTest(text, expect)
  def test_IOspecDest1(self):
    text = 'task (>dest)'
    expect = self.makeDict(name='task', dest=['dest'])
    self.parseTest(text, expect)
  def test_IOspecDestN(self):
    text = 'task (>dest1 dest2)'
    expect = self.makeDict(name='task', dest=['dest1', 'dest2'])
    self.parseTest(text, expect)
  def test_IOspecFull(self):
    text = 'task (arg1, arg2 > dest1 dest2)'
    expect = self.makeDict(name='task', args=['arg1', 'arg2'], dest=['dest1', 'dest2'])
    self.parseTest(text, expect)
  def test_complete(self):
    text = 'task (arg1 arg2 > dest) param1 param2'
    expect = self.makeDict(name='task', args=['arg1', 'arg2'], dest=['dest'], params=['param1', 'param2'])
    self.parseTest(text, expect)

if __name__=="__main__":
  print("Running parser tests...")
  unittest.main()
