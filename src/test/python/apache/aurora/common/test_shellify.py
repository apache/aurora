from apache.aurora.common.shellify import shellify


def test_shellify():
  dump = list(shellify(
    {
      "num": 123,
      "string": "abc",
      "obj": {
        "num": 456,
        "string": "def",
        "description": "Runs the world",
      },
      "arr": [
        7,
        "g",
        {
          "hi": [0],
        },
      ]
    }
  , prefix="TEST_"))

  assert set(dump) == set([
    "TEST_NUM=123",
    "TEST_STRING=abc",
    "TEST_OBJ_NUM=456",
    "TEST_OBJ_STRING=def",
    "TEST_OBJ_DESCRIPTION='Runs the world'",
    "TEST_ARR_0=7",
    "TEST_ARR_1=g",
    "TEST_ARR_2_HI_0=0",
  ])
