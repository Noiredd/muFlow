import enum

from errors import *

class BaseToken(object):
  def __init__(self, text):
    self.text = text
class Literal(BaseToken):
  def __init__(self, text):
    self.text = text
class Brack_L(BaseToken):
  def __init__(self, text):
    self.text = text
class Brack_R(BaseToken):
  def __init__(self, text):
    self.text = text
class Dest_Op(BaseToken):
  def __init__(self, text):
    self.text = text

class PState(enum.Enum):
  NAME = 0
  IO_OR_PARAM = 1
  INPUT = 2
  OUTPUT = 3
  PARAM = 4
  END = 5
class LState(enum.Enum):
  CHAR = 0
  CTRL = 1
  QUOT = 2
class CType(enum.Enum):
  CHAR = 0
  CTRL = 1
  QUOTE = 2
  WHITE = 3
  COMNT = 4

class Lexer(object):
  control_map = {
    '(': Brack_L,
    ')': Brack_R,
    '>': Dest_Op
  }
  control_chars = ''.join(control_map.keys())
  comment_chars = ['#']
  white_chars = [' ', '\t', ',']
  quote_chars = ['"']
  matrix = {
    LState.CHAR: {
      #char type:  (push?  token? new state?)
      CType.CHAR:  (True,  False, LState.CHAR),
      CType.CTRL:  (True,  True,  LState.CTRL),
      CType.QUOTE: (False, True,  LState.QUOT),
      CType.WHITE: (False, True,  LState.CHAR),
      CType.COMNT: (False, True,  None)
    },
    LState.CTRL: {
      CType.CHAR:  (True,  True,  LState.CHAR),
      CType.CTRL:  (True,  True,  LState.CTRL),
      CType.QUOTE: (False, True,  LState.QUOT),
      CType.WHITE: (False, True,  LState.CHAR),
      CType.COMNT: (False, True,  None)
    },
    LState.QUOT: {
      CType.CHAR:  (True,  False, LState.QUOT),
      CType.CTRL:  (True,  False, LState.QUOT),
      CType.QUOTE: (False, True,  LState.CHAR),
      CType.WHITE: (True,  False, LState.QUOT),
      CType.COMNT: (True,  False, LState.QUOT)
    }
  }

  def __recognizeChar(self, ch):
    if ch in self.white_chars:
      return CType.WHITE
    elif ch in self.quote_chars:
      return CType.QUOTE
    elif ch in self.control_chars:
      return CType.CTRL
    elif ch in self.comment_chars:
      return CType.COMNT
    else:
      return CType.CHAR

  def __pushChar(self, ch):
    self.literal += ch

  def __pushToken(self):
    if self.literal == '':
      return
    # construct a token of the right type depending on the text
    if self.literal in self.control_map.keys():
      tok_class = self.control_map[self.literal]
      self.tokens.append(tok_class(self.literal))
    elif not self.literal in self.comment_chars:
      # ignore the comment symbols - the parsing ends here
      self.tokens.append(Literal(self.literal))
    self.literal = ''

  def tokenize(self, text):
    self.tokens = []
    self.literal = ''
    state = LState.CHAR
    for ch in text:
      # obtain the rule for a given state and encountered character
      state_rule = self.matrix[state]
      char_type = self.__recognizeChar(ch)
      push_char, push_token, state = state_rule[char_type]
      # push the complete token to the list
      if push_token:
        self.__pushToken()
      # token is a comment symbol
      if state is None:
        break
      # push the character to the token being built
      if push_char:
        self.__pushChar(ch)
    self.__pushToken()
    return self.tokens

class Parser(object):
  matrix = {
    PState.NAME: {
      Literal: ('name',   PState.IO_OR_PARAM)
    },
    PState.IO_OR_PARAM: {
      Brack_L: (None,     PState.INPUT),
      Literal: ('params', PState.PARAM)
    },
    PState.INPUT: {
      Literal: ('args',   PState.INPUT),
      Dest_Op: (None,     PState.OUTPUT),
      Brack_R: (None,     PState.PARAM)
    },
    PState.OUTPUT: {
      Literal: ('dest',   PState.OUTPUT),
      Brack_R: (None,     PState.PARAM)
    },
    PState.PARAM: {
      Literal: ('params', PState.PARAM)
    }
  }

  def __init__(self):
    self.lexer = Lexer()

  def parseText(self, text):
    tokens = self.lexer.tokenize(text)
    return self.parseTokens(tokens)

  def parseTokens(self, tokens):
    state = PState.NAME
    parsed = {
      'name': '',
      'args': [],
      'dest': [],
      'params': []
    }
    for token in tokens:
      state_rule = self.matrix[state]
      token_type = type(token)
      if token_type not in state_rule.keys():
        raise ParsingException('Unexpected token type') # TODO: nice printouts with pointing to offender
      token_key, state = state_rule[token_type]
      if token_key is not None:
        if token_key == 'name':
          parsed['name'] = token.text
        else:
          parsed[token_key].append(token.text)
    return parsed
