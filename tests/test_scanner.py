"""Unit tests for holland_restore.scanner"""

from holland_restore.scanner import Scanner
from nose.tools import assert_equals, assert_raises

def test_scanner_pushback():
    """Test scanner push_back"""
    lines = [
        "foo\n",
        "bar\n",
        "baz\n",
    ]
    scanner = Scanner(lines)

    i = iter(scanner)
    first_token = i.next()
    assert_equals(first_token, lines[0])
    scanner.push_back(first_token)
    second_token = i.next()
    assert_equals(second_token, first_token)
    third_token = i.next()
    assert_equals(third_token, lines[1])
    fourth_token = i.next()
    assert_equals(fourth_token, lines[2])
    assert_raises(StopIteration, i.next)
    scanner.push_back(fourth_token)
    fifth_token = i.next()
    assert_equals(fourth_token, fifth_token)

def test_scanner_position():
    """Test scanner line counts and byte offsets"""
    lines = [
        "foo\n",
        "bar\n",
        "baz\n",
    ]

    scanner = Scanner(lines)
    i = iter(scanner)
    # initial position
    assert_equals(scanner.position, (0, 0))
    i.next()
    assert_equals(scanner.position, (1, 0))
    token = i.next()
    assert_equals(scanner.position, (2, 4))
    scanner.push_back(token)
    assert_equals(scanner.position, (1, 0))

def test_seekable_source():
    """Test scanner + StringIO
    """
    import textwrap
    from StringIO import StringIO
    lines = textwrap.dedent("""
    foo
    bar
    baz
    biz
    """).lstrip()

    data = StringIO(lines)
    scanner = Scanner(data)
    results = []
    for atom in scanner:
        results.append((atom, scanner.position))
    for i, (atom, (linenum, offset)) in enumerate(results):
        data.seek(offset)
        sought_atom = data.read(len(atom))
        assert_equals(sought_atom, atom)
        assert_equals(linenum, i + 1)
