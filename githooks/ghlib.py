import os
import shlex
import subprocess

# -----------------------------------------------------------------------------
def catch_stdout(cmd, input=None):
    """
    Run *cmd*, optionally passing string *input* to it on stdin, and return
    what the process writes to stdout
    """
    try:
        p = subprocess.Popen(shlex.split(cmd),
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        if input:
            p.stdin.write(input)
        (o, e) = p.communicate()
        if p.returncode == 0:
            rval = o
        else:
            rval = 'ERR:' + e
    except OSError as e:
        if 'No such file or directory' in str(e):
            rval = 'ERR:' + str(e)
        else:
            raise

    return rval


# -----------------------------------------------------------------------------
def contents(filename):
    """
    Read a file and return its contents as a list of lines with newlines
    stripped off.
    """
    f = open(filename, 'rU')
    rval = [z.rstrip('\n') for z in f.readlines()]
    f.close()
    return rval


# -----------------------------------------------------------------------------
def get_change_id(msg):
    """
    Generate a change id line based on the commit message and return it
    """
    istr = 'tree '
    istr += catch_stdout('git write-tree')
    parent = catch_stdout('git rev-parse "HEAD^0"')
    if not parent.startswith('ERR:'):
        istr += 'parent ' + parent
    istr += 'author ' + catch_stdout('git var GIT_AUTHOR_IDENT')
    istr += 'committer ' + catch_stdout('git var GIT_COMMITTER_IDENT')
    istr += '\n'.join(msg)
    rval = catch_stdout('git hash-object -t commit --stdin',
                        input=istr)
    return 'Change-Id: I' + rval


# -----------------------------------------------------------------------------
def get_version():
    """
    Scan the current git repo for a file named 'version.py'. We assume it
    contains a statement that sets __version__ and we use that to construct and
    return a line of the form 'Version:    x.x.x'
    """
    vpath = get_version_path()
    vinfo = contents(vpath)
    eval(compile('\n'.join(vinfo), vpath, 'exec'))
    rval = "Version:   %s" % locals()["__version__"]
    return rval


# -----------------------------------------------------------------------------
def get_version_path():
    """
    Scan the current git repo for a file named 'version.py'. Return the path to
    it.
    """
    groot = catch_stdout('git rev-parse --show-toplevel')
    if groot.startswith('ERR:'):
        groot = '.'
    for r, d, f in os.walk(groot.strip()):
        if 'version.py' in f:
            vpath = os.path.join(r, 'version.py')
            break
    return vpath


# -----------------------------------------------------------------------------
def get_version_ht():
    """
    Get the version string and return the head and tail. The head is the first
    two segments. The tail is the third segment or the empty string if there is
    no third segment.
    """
    vs = get_version_string()
    vl = vs.split('.')
    head = '.'.join(vl[0:2])
    if 2 < len(vl):
        tail = int(vl[2])
    else:
        tail = 0
    return (vs, head, tail)


# -----------------------------------------------------------------------------
def get_version_string():
    """
    Scan the current git repo for a file named 'version.py'. We assume it
    contains a statement that sets __version__. We return the value of
    __version__.
    """
    vpath = get_version_path()
    vinfo = contents(vpath)
    eval(compile('\n'.join(vinfo), vpath, 'exec'))
    return locals()['__version__']

# -----------------------------------------------------------------------------
def git_describe_ht():
    """
    Run 'git describe' in the current repo. There are three possible results:
        '' - head = '', tail = 0
        '2014.1116-9-g1eaeaad' - head = '2014.1116', tail = 9
        '2015.0125' - head = '2015.0125', tail = 0
    """
    r = catch_stdout('git describe')
    if 'No names found' in r:
        full = ''
        head = ''
        tail = 0
    elif '-' in r:
        rl = r.split('-')
        full = '.'.join([rl[0], rl[1]])
        head = rl[0]
        tail = int(rl[1])
    else:
        full = head = r.strip()
        tail = 0
    return((full, head, tail))


# -----------------------------------------------------------------------------
def save_new(filename, payload, version, cid, comments):
    """
    Write *payload*, *version*, *cid*, and *comments* to *filename*.new,
    returning the new name
    """
    newname = filename + ".new"
    o = open(newname, 'w')
    o.writelines([p + '\n' for p in payload])
    if 0 < len(payload[-1]):
        o.write("\n")
    o.writelines([version.strip() + '\n'])
    o.writelines([cid.strip() + '\n'])
    o.writelines([p + '\n' for p in comments])
    o.close()
    return newname


# -----------------------------------------------------------------------------
def select(needle, haystack):
    """
    Look for string *needle* in list of strings *haystack*. Return the first
    matching string from *haystack*.
    """
    r = [x for x in haystack if needle in x]
    if r:
        return r[0]
    else:
        return ''


# -----------------------------------------------------------------------------
def split_msg(msg):
    """
    Scan *msg* and split it into payload, version line, change id line, and
    comments and return those components
    """
    payload = []
    comments = []
    version = ''
    cid = ''
    for l in msg:
        if l.startswith('#'):
            comments.append(l)
        elif 'Version:' in l:
            version = l
        elif 'Change-Id:' in l:
            cid = l
        else:
            payload.append(l)
    return(payload, version, cid, comments)
