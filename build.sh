#!/usr/bin/env python

import contextlib
import fnmatch
import glob
from HTMLParser import HTMLParser
import optparse
import os
import pdb
import pexpect
import re
import requests
import shutil
import sys
import time

# -----------------------------------------------------------------------------
def main(args):
    """
    Generate hpssic rpm and an rpm for each of its requirements
    """
    global opt
    (opt, a) = argz(args, [{'names': ['-d', '--debug'],
                            'action': 'store_true',
                            'default': False,
                            'dest': 'debug',
                            'help': 'run the debugger'
                            },
                           {'names': ['-o', '--only'],
                            'action': 'append',
                            'default': [],
                            'dest': 'only',
                            'help': 'steps to do'
                            },
                           {'names': ['-r', '--reset'],
                            'action': 'store_true',
                            'default': False,
                            'dest': 'reset',
                            'help': 'clear the build area'
                            },
                           {'names': ['-s', '--skip'],
                            'action': 'append',
                            'default': [],
                            'dest': 'skips',
                            'help': 'steps to skip'
                            },
                           {'names': ['-t', '--test'],
                            'action': 'store_true',
                            'default': False,
                            'dest': 'test',
                            'help': 'run the test routine'
                            },
                           {'names': ['-n', '--dry-run'],
                            'action': 'store_true',
                            'default': False,
                            'dest': 'dryrun',
                            'help': 'report without doing'
                            }
                           ])

    (dstdir, blddir) = prepdir(['dist', 'build'])
    reqs = {'ibmdb': {'name': 'ibm_db',
                      'cmds': [set_ibm_home, set_cflags, edit_setup],
                      },
            # 'pytest': {'name': 'pytest',
            #            'cmds': [],
            #            },
            # 'mysql': {'name': 'MySQL-python',
            #           'cmds': [prep_mysql],
            #           },
            # 'pexpect': {'name': 'pexpect',
            #             'cmds': []
            #             },
            }

    everything = reqs.keys() + ['hpssic']
    if opt.only:
        reqlist = [x for x in opt.only if x in everything]
    elif opt.skips:
        reqlist = [x for x in everything if x not in opt.skips]
    else:
        reqlist = everything

    # set_ibm_home()
    if opt.test:
        test_build_sh()
    elif opt.reset:
        shutil.rmtree(blddir)
        shutil.rmtree(dstdir)
    else:
        run(output="build/build.sh.out")
        if 'hpssic' in reqlist:
            run("python setup.py bdist_rpm")
            reqlist.remove('hpssic')
        with cd(blddir):
            for req in reqlist:
                make_rpm(blddir, dstdir, reqs[req])


# -----------------------------------------------------------------------------
def argz(argv, plist):
    """
    Parse the command line
    """
    p = optparse.OptionParser(epilog='For -o and -s, recognized arguments'
                              " are 'ibmdb', 'mysql', 'pexpect', and 'pytest'")
    debug_present = False
    for arg in plist:
        if '--debug' in arg['names']:
            debug_present = True
        names = arg['names']
        k = dict((q, arg[q]) for q in arg if q != 'names')
        p.add_option(*arg['names'], **k)

    if not debug_present:
        p.add_option('-d', '--debug',
                     action='store_true', default=False, dest='debug',
                     help='run the debugger')

    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    return(o, a)


# -----------------------------------------------------------------------------
@contextlib.contextmanager
def cd(path):
    origin = os.getcwd()
    os.chdir(path)

    yield

    os.chdir(origin)


# -----------------------------------------------------------------------------
def compute_url(reqname):
    """
    Figure out the url to retrieve a .tar.gz file for *reqname*
    """
    durl = ("https://pypi.python.org/packages/source/%s/%s/" %
            (reqname[0], reqname))
    d = requests.get(durl)
    h = html()
    h.feed(d.content)
    recent = h.result()
    purl = durl + recent
    return(purl)


# -----------------------------------------------------------------------------
def deliver_rpms(target):
    """
    copy any rpms generated up to the target directory
    """
    for item in glob.glob("dist/*.rpm"):
        shutil.copy(item, target)


# -----------------------------------------------------------------------------
def expand_manifest(in_name, out_name):
    """
    Expand MANIFEST.in to MANIFEST
    """
    with open(in_name, 'r') as r:
        with open(out_name, 'w') as w:
            for line in r:
                if line.startswith("recursive-include"):
                    (c, rd, pat) = line.strip().split()
                    for r, d, f in os.walk(rd):
                        for name in fnmatch.filter(f, pat):
                            w.write("%s/%s\n" % (r, name))
                elif line.startswith("include"):
                    (c, fname) = line.strip().split()
                    w.write(fname + '\n')
                else:
                    w.write(line.rstrip() + "\n")


# -----------------------------------------------------------------------------
def fetch(req):
    """
    Retrieve a package from the web
    """
    url = compute_url(req)
    rval = os.path.basename(url)
    run("wget --no-check-certificate %s" % url)
    return rval


# -----------------------------------------------------------------------------
def make_rpm(bld, dst, rdict):
    """
    Retrieve requirement *req*, unpack it, use python setup.py to generate rpms
    and move them to *target*. Clean up after yourself.
    """
    filename = fetch(rdict['name'])
    p = unpack(filename)
    with cd(p):
        for func in rdict['cmds']:
            func()
        run("python setup.py bdist_rpm")
        deliver_rpms(dst)


# -----------------------------------------------------------------------------
def prep_mysql():
    """
    Get mysql ready for bdist_rpm. Things we've found that have to be done so
    far:
     - comment the doc_files line in mysql's setup.cfg
    """
    filename = 'setup.cfg'
    with open(filename, 'r') as r:
        with open(filename + '.new', 'w') as w:
            for line in r:
                if 'doc_files' in line:
                    w.write("# " + line)
                else:
                    w.write(line)
    os.rename(filename, filename + '.old')
    os.rename(filename + '.new', filename)


# -----------------------------------------------------------------------------
def prepdir(dl):
    """
    Make sure *path* exists and is a directory. Unless opt.reset is true. In
    that case, we're deleting the directories if they exist, so don't create
    them.
    """
    global opt
    rval = []
    for dname in dl:
        path = os.path.join(os.getcwd(), dname)
        if not os.path.exists(path) and not opt.reset:
            os.mkdir(path)
        elif not os.path.isdir(path):
            raise SystemExit("%s is not a directory" % path)
        rval.append(path)
    return rval


# -----------------------------------------------------------------------------
def run(cmd=None, output=None):
    """
    Run *cmd* or just display it, depending on the dryrun option
    """
    global opt

    if output:
        run._output = open(output, 'w')

    try:
        x = run._output
    except:
        run._output = sys.stdout

    if cmd is not None:
        print(cmd)
        if run._output != sys.stdout:
            run._output.write("$ " + cmd + "\n")
        if not opt.dryrun:
            run._output.write(pexpect.run(cmd, logfile=run._output))


# -----------------------------------------------------------------------------
def edit_setup():
    """
    In setup.py, we have to change './CHANGES' to '$PWD/CHANGES' in the
    data_files list so the rpm build can find the file.
    """
    rgx = '\./(README|CHANGES|LICENSE)'
    with open('setup.py', 'r') as r:
        with open('setup.py.new', 'w') as w:
            for line in r:
                if re.findall(rgx, line):
                    # w.write(re.sub(rgx, '%s/\g<1>' % os.getcwd(), line))
                    w.write("# " + line)
                else:
                    w.write(line)
    os.rename('setup.py', 'setup.py.old')
    os.rename('setup.py.new', 'setup.py')


# -----------------------------------------------------------------------------
def set_cflags():
    """
    Have to add the current directory to the list of include files. We do this
    by writing a new setup.cfg file.
    """
    with open('setup.cfg', 'w') as f:
        f.write("[build_ext]\n")
        f.write("include_dirs = %s\n" % os.getcwd())
        # f.write("\n")
        # f.write('[files]\n')
        # f.write('extra_files =\n')
        # f.write('    %s/README\n' % os.getcwd())
        # f.write('    %s/CHANGES\n' % os.getcwd())
        # f.write('    %s/LICENSE\n' % os.getcwd())

# -----------------------------------------------------------------------------
def set_ibm_home():
    """
    Set the environment variable IBM_DB_HOME
    """
    l = glob.glob("/opt/ibm/db2/*")
    bn = [os.path.basename(q) for q in l]
    f = [q.lstrip('vV') for q in bn]
    d = dict((float(k), v) for k, v in zip(f, l))
    i = sorted(d.keys())[-1]
    os.environ['IBM_DB_HOME'] = d[i]

    
# -----------------------------------------------------------------------------
def test_build_sh():
    """
    Test routine for html parsing (or anything else we want to try out)
    """
    with cd("build/MySQL-python-1.2.5"):
        expand_manifest("MANIFEST.in", "MANIFEST")


# -----------------------------------------------------------------------------
def unpack(filename):
    """
    Unpack a file, either .tar.gz or .zip
    """
    if filename.endswith('.tar.gz'):
        run("tar xf %s" % filename)
        rval = filename.replace('.tar.gz', '')
    elif filename.endswith('.zip'):
        run("unzip %s" % filename)
        rval = filename.replace('.zip', '')
    else:
        print("I don't know how to unpack %s" % filename)
        rval = ''
    return rval


# -----------------------------------------------------------------------------
class html(HTMLParser, object):
    def __init__(self, *args, **kw):
        super(html, self).__init__(*args, **kw)
        self._html_result = []
        self.current = None

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            ad = dict(attrs)
            if 'href' in ad:
                name = ad['href']
                if re.findall("(.tar.gz|.zip)$", name):
                    self.current = {'name': name}
                    self._html_result.append(self.current)
                else:
                    self.current = None

    def handle_data(self, data):
        # print("handle_data got '%s'" % data)
        if self.current:
            try:
                d = ' '.join(data.split()[0:2])
                tm = time.strptime(d.strip(), "%d-%b-%Y %H:%M")
                self.current['date'] = time.strftime("%Y.%m%d %H:%M", tm)
            except ValueError:
                pass

    def result(self):
        if 0 < len(self._html_result):
            rval = sorted(self._html_result,
                          lambda a,b: cmp(a['date'], b['date']))[-1]['name']
        else:
            rval = ''
        return rval


# -----------------------------------------------------------------------------
if __name__ == '__main__':
    main(sys.argv)
