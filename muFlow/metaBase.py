class MetaBase(type):
  def __new__(cls, name, bases, dct):
    x = super(MetaBase, cls).__new__(cls, name, bases, dct)
    # Give each class its own validness flag, false by default,
    # so that Assembler properly validates the parameters
    x.isValid = False
    # Distinguish base classes from derived ones to prevent
    # Assembler from adding them to the construction lists
    if not 'isBase' in dct.keys():
      x.isBase = False
    return x
