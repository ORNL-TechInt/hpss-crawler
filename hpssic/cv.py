#!/usr/bin/env python
import Checkable
import crawl_lib
import CrawlConfig
import CrawlDBI
import Dimension
import hpss
import optparse
import os
import pdb
import pexpect
from pprint import pprint
import re
import time
import toolframe

prefix = "cv"
H = None


# -----------------------------------------------------------------------------
def cv_addcart(argv):
    """addcart - Add field cart to checkable table

    Add text field cart to table <CTX>_checkables.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    db = CrawlDBI.DBI()
    table = db._dbobj.prefix("checkables")
    cmd = "alter table %s add column cart text after cos" % table
    db.sql(cmd)
    db.close()


# -----------------------------------------------------------------------------
def cv_dropcart(argv):
    """dropcart - Drop field cart from checkable table

    Drop the field cart from the checkable table
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    db = CrawlDBI.DBI()
    table = db._dbobj.prefix("checkables")
    cmd = "alter table %s drop column cart" % table
    db.sql(cmd)
    db.close()


# -----------------------------------------------------------------------------
def cv_popcart(argv):
    """popcart - Populate field cart in checkable table

    usage: cv popcart [-d] [path ...]

    For each file path in the mysql database that doesn't already have a cart
    name, issue an 'ls -P <path>' in hsi to retrieve the name of the cartridge
    where the file is stored. Record the cartridge name in the database.

    If path name(s) are specified on the command line, only the records for
    those paths will be updated with a cart name.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-m', '--max',
                 action='store', default="0", dest='max',
                 help='stop after doing this many records')
    p.add_option('-n', '--dry-run',
                 action='store_true', default=False, dest='dryrun',
                 help='show which rows would be updated')
    p.add_option('-s', '--skip',
                 action='store_true', default=False, dest='skip',
                 help='skip rows with non-null cart')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    db = CrawlDBI.DBI()
    if o.skip:
        where = "type = 'f' and last_check <> 0 and cart is null"
    else:
        where = "type = 'f' and last_check <> 0"

    path_l = db.select(table="checkables",
                       fields=["path", "cart"],
                       where=where)

    h = hpss.HSI(verbose=True)
    if len(a) <= 0:
        for (path, cart) in path_l:
            if populate_cart_field(db, h, path, cart,
                                   int(o.max), o.dryrun):
                break
    else:
        path_d = dict(path_l)
        for path in a:
            if path in path_d:
                if populate_cart_field(db, h, path, path_d[path],
                                       int(o.max), o.dryrun):
                    break
            else:
                print("NOTINDB %-8s %s" % (" ", path))
    h.quit()
    db.close()


# -----------------------------------------------------------------------------
def populate_cart_field(db, h, path, dbcart, max, dryrun):
    info = h.lsP(path)
    cartname = info.split("\t")[5].strip()
    if dbcart != cartname:
        if 0 < max:
            try:
                populate_cart_field._count += 1
            except AttributeError:
                populate_cart_field._count = 1
            if max < populate_cart_field._count:
                return True
        if dryrun:
            print("would set %s %s" % (cartname, path))
        else:
            print("setting %s %s" % (cartname, path))
            db.update(table="checkables",
                      fields=['cart'],
                      where="path = ?",
                      data=[(cartname, path)])
    else:
        print("ALREADY %s %s" % (dbcart, path))
    return False


# -----------------------------------------------------------------------------
def cv_addcart(argv):
    """addcart - Add field cart to checkable table

    Add text field cart to table <CTX>_checkables.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    db = CrawlDBI.DBI()
    table = db._dbobj.prefix("checkables")
    cmd = "alter table %s add column cart text after cos" % table
    db.sql(cmd)
    db.close()


# -----------------------------------------------------------------------------
def cv_dropcart(argv):
    """dropcart - Drop field cart from checkable table

    Drop the field cart from the checkable table
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    db = CrawlDBI.DBI()
    table = db._dbobj.prefix("checkables")
    cmd = "alter table %s drop column cart" % table
    db.sql(cmd)
    db.close()


# -----------------------------------------------------------------------------
def cv_popcart(argv):
    """popcart - Populate field cart in checkable table

    usage: cv popcart [-d] [path ...]

    For each file path in the mysql database that doesn't already have a cart
    name, issue an 'ls -P <path>' in hsi to retrieve the name of the cartridge
    where the file is stored. Record the cartridge name in the database.

    If path name(s) are specified on the command line, only the records for
    those paths will be updated with a cart name.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-m', '--max',
                 action='store', default="0", dest='max',
                 help='stop after doing this many records')
    p.add_option('-n', '--dry-run',
                 action='store_true', default=False, dest='dryrun',
                 help='show which rows would be updated')
    p.add_option('-s', '--skip',
                 action='store_true', default=False, dest='skip',
                 help='skip rows with non-null cart')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    db = CrawlDBI.DBI()
    if o.skip:
        where = "type = 'f' and last_check <> 0 and cart is null"
    else:
        where = "type = 'f' and last_check <> 0"

    path_l = db.select(table="checkables",
                       fields=["path", "cart"],
                       where=where)

    h = hpss.HSI(verbose=True)
    if len(a) <= 0:
        for (path, cart) in path_l:
            if populate_cart_field(db, h, path, cart,
                                   int(o.max), o.dryrun):
                break
    else:
        path_d = dict(path_l)
        for path in a:
            if path in path_d:
                if populate_cart_field(db, h, path, path_d[path],
                                       int(o.max), o.dryrun):
                    break
            else:
                print("NOTINDB %-8s %s" % (" ", path))
    h.quit()
    db.close()


# -----------------------------------------------------------------------------
def populate_cart_field(db, h, path, dbcart, max, dryrun):
    info = h.lsP(path)
    cartname = info.split("\t")[5].strip()
    if dbcart != cartname:
        if 0 < max:
            try:
                populate_cart_field._count += 1
            except AttributeError:
                populate_cart_field._count = 1
            if max < populate_cart_field._count:
                return True
        if dryrun:
            print("would set %s %s" % (cartname, path))
        else:
            print("setting %s %s" % (cartname, path))
            db.update(table="checkables",
                      fields=['cart'],
                      where="path = ?",
                      data=[(cartname, path)])
    else:
        print("ALREADY %s %s" % (dbcart, path))
    return False


# -----------------------------------------------------------------------------
def cv_report(argv):
    """report - show the checksum verifier database status

    select count(*) from checkables where type = 'f';
    select count(*) from checkables where checksum <> 0;
    """
    p = optparse.OptionParser()
    p.add_option('-c', '--cfg',
                 action='store', default='', dest='config',
                 help='config file name')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-p', '--prefix',
                 action='store', default='', dest='prefix',
                 help='table name prefix')
    p.add_option('-v', '--verbose',
                 action='store_true', default=False, dest='verbose',
                 help='pass verbose flag to HSI object')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    if o.config != '':
        cfg = CrawlConfig.get_config(o.config)
    else:
        cfg = CrawlConfig.get_config()

    if o.prefix != '':
        cfg.set('dbi', 'tbl_prefix', o.prefix)

    dim = {}
    dim['cos'] = Dimension.get_dim('cos')
    dim['cart'] = Dimension.get_dim('cart')

    print dim['cos'].report()
    print dim['cart'].report()


# -----------------------------------------------------------------------------
def cv_nulltest(argv):
    """nulltest - how do NULL values show up when queried?

    After doing 'alter table dev_checkables add fails int', the added fields
    (fails, reported, cart) are NULL, not 0. What does that look like when I
    select a record?
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-f', '--filename',
                 action='store', default='hpss_crawl.log', dest='filename',
                 help='name of log file')
    p.add_option('-v', '--verbose',
                 action='store_true', default=False, dest='verbose',
                 help='pass verbose flag to HSI object')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    db = CrawlDBI.DBI()

    rows = db.select(table='checkables',
                     where="fails is null or reported is null or cart is null")
    hfmt = "%5s %-45s %4s %-4s %-8s %3s %11s %3s %3s"
    rfmt = "%5d %-47.47s %-2s %4s %-8s %2d  %11d %2d  %2d"
    headers = ("Rowid", "Path", "Type", "COS", "Cart", "Chk", "Last Check",
               "Fls", "Rpt")
    print(hfmt % headers)
    rcount = 0
    for r in rows:
        print(rfmt % r)
        rcount += 1
        if 50 < rcount:
            print(hfmt % headers)
            rcount = 0

    db.close()


# -----------------------------------------------------------------------------
def cv_fail_reset(argv):
    """fail_reset - reset a failing path so it can be checked again

    After doing 'alter table dev_checkables add fails int', the added fields
    (fails, reported) are NULL, not 0. What does that look like when I select a
    record?
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-p', '--pathname',
                 action='store', default='', dest='pathname',
                 help='name of path to be reset')
    p.add_option('-v', '--verbose',
                 action='store_true', default=False, dest='verbose',
                 help='pass verbose flag to HSI object')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    if o.pathname == '':
        print("pathname is required")
        return

    db = CrawlDBI.DBI()

    db.update(table='checkables',
              fields=['fails', 'reported'],
              where="path = ?",
              data=[(0, 0, o.pathname)])

    db.close()


# -----------------------------------------------------------------------------
def cv_show_next(argv):
    """show_next - Report the Checkables in the order they will be checked

    usage: cvtool shownext
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-i', '--id',
                 action='store', default='', dest='id',
                 help='id of entry to be checked')
    p.add_option('-p', '--path',
                 action='store', default='', dest='path',
                 help='name of path to be checked')
    p.add_option('-v', '--verbose',
                 action='store_true', default=False, dest='verbose',
                 help='more information')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    clist = Checkable.Checkable.get_list()
    for c in clist:
        if c.last_check == 0:
            print("%18d %s %s" % (c.last_check,
                                  c.type,
                                  c.path))
        else:
            print("%s %s %s" % (ymdhms(c.last_check),
                                c.type,
                                c.path))


# -----------------------------------------------------------------------------
def cv_simplug(argv):
    """simplug - Simulate running a plugin

    usage: cv simplug [-d] [-i <iterations>]

    Simulate running the checksum-verifier plugin
    """
    crawl_lib.simplug('cv', argv)


# -----------------------------------------------------------------------------
def cv_test_check(argv):
    """test_check - Run Checkable.check() on a specified entry

    usage: cvtool test_check [-p/--path PATH] [-i/--id ROWID]

    The options are mutually exclusive.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-i', '--id',
                 action='store', default='', dest='id',
                 help='id of entry to be checked')
    p.add_option('-p', '--path',
                 action='store', default='', dest='path',
                 help='name of path to be checked')
    p.add_option('-v', '--verbose',
                 action='store_true', default=False, dest='verbose',
                 help='more information')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    if o.path != '' and o.id != '':
        print("Only --path or --id is allowed, not both.")
        return
    elif o.path != '':
        c = Checkable.Checkable(path=o.path)
    elif o.id != '':
        c = Checkable.Checkable(rowid=int(o.id))
    else:
        print("One of --path or --id is required.")
        return

    c.load()
    c.check()


# -----------------------------------------------------------------------------
def cv_ttype_add(argv):
    """ttype_add - Add field 'ttype' to the checkables table

    usage: cv ttype_add [-d]
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    # lookup and report tape type for each pathname specified


# -----------------------------------------------------------------------------
def cv_ttype_drop(argv):
    """ttype_drop - Drop field 'ttype' from the checkables table

    usage: cv ttype_drop [-d]
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    # lookup and report tape type for each pathname specified


# -----------------------------------------------------------------------------
def cv_ttype_lookup(argv):
    """ttype_lookup - Look up the tape type for a specified pathname

    usage: cv ttype_lookup [-d] path ...
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    # lookup and report tape type for each pathname specified


# -----------------------------------------------------------------------------
def cv_ttype_populate(argv):
    """ttype_populate - populate the ttype field in checkables

    usage: cv ttype_populate [-d] [-a] [path ...]

    If one or more paths are specified, those records will be looked up in
    checkables and populated with media type information if present. If no
    paths are specified and -a/--all is, all records in checkables will be
    updated with media type info.
    """
    p = optparse.OptionParser()
    p.add_option('-a', '--all',
                 action='store_true', default=False, dest='all',
                 help='process all records in checkables')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    # lookup and report tape type for each pathname specified


# -----------------------------------------------------------------------------
def cv_ttype_table(argv):
    """ttype_table - create (or drop) table tape_types

    usage: cv ttype_table [-d] {-D|-r /opt/hpss}

    Without the -D/--drop option, create the table tape_types in the mysql
    database. Populate it with information from an HPSS build tree (default is
    /opt/hpss).

    With -D or --drop, drop the table.
    """
    p = optparse.OptionParser()
    p.add_option('-D', '--drop',
                 action='store_true', default=False, dest='drop',
                 help='drop the table')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-r', '--root',
                 action='store', default='', dest='hpssroot',
                 help='where to look for data')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    db = CrawlDBI.DBI()
    # lookup and report tape type for each pathname specified
    if o.drop:
        db.drop(table="tape_types")
    else:
        if not db.table_exists(table="tape_types"):
            db.create(table="tape_types",
                      fields=["type int",
                              "subtype int",
                              "name text"])

        hpssroot = o.hpssroot
        if hpssroot == '':
            hpssroot = os.getenv("HPSS_ROOT")
        if hpssroot is None:
            hpssroot = "/opt/hpss"

        tape_types_populate(hpssroot)


# -----------------------------------------------------------------------------
def tape_types_populate(hpssroot):
    """
    {'MT_TAPE_3590':
        {'val': '0x01000005',
         '0x00000000': ['MT_SUB_3590_O', 'MT_SUB_3590_SINGLELEN',
                        'MT_SUB_3590_O_SINGLELEN']
         '0x00000001': ['MT_SUB_3590_E', 'MT_SUB_3590_E_SINGLELEN']

    """
    media_type_file = os.path.join(hpssroot, "include", "hpss_media_type.x")
    msg_file = os.path.join(hpssroot, "include", "cs_LogErr.h")

    mt = open(media_type_file, "r")
    hint = lambda x: int(x.replace(";", ""), 0)
    TT = {}
    rait_types = []

    def dubk(d, x, y, z):
        try:
            d[x][y]['list'].append(z)
        except KeyError, e:
            d[x][y] = {'list': [z]}

    for line in mt.readlines():
        x = line.split()
        if "const MT_TAPE_" in line:
            name = x[1]
            t = name.replace("MT_TAPE_", "")
            val = hint(x[3])
            TT[val] = {'name': name}
            TT[t] = TT[val]
            continue

        if "const MT_RAIT_" in line:
            name = x[1]
            t = name.replace("MT_", "")
            val = hint(x[3])
            TT[val] = {'name': name}
            TT[t] = TT[val]
            rait_types.append(t)
            continue

        if "const MT_SUB_RAIT_" in line:
            name = x[1]
            val = hint(x[3])
            for rt in rait_types:
                dubk(TT, rt, val, name)
            continue

        if "const MT_SUB_" in line:
            name = x[1]
            val = hint(x[3])
            t = name.split("_")[2]
            if t in ["SMALL", "MEDIUM", "LARGE"]:
                for t in ["DD2", "DST"]:
                    dubk(TT, t, val, name)
            elif t in ["2", "5GB"]:
                dubk(TT, "8MM", val, name)
            else:
                dubk(TT, t, val, name)

    mt.close()

    msg = open(msg_file, "r")
    for line in msg.readlines():
        if all(["MT_TAPE_" in line, "_DEF " in line]):
            x = line.strip().split(None, 2)
            name = x[1]
            t = name.split("_")[2]
            # t = name.replace("_DEF", "")
            # t = re.sub("_DEF$", "", name)
            label = x[2].strip('"').replace("\\n", "")
            TT[t]['label'] = label
            continue

        if all(["MT_SUB_" in line, "_DEF " in line]):
            x = line.strip().split(None, 2)
            name = x[1]
            t = re.sub("_DEF$", "", name)
            k = name.split("_")[2]
            if k in ['2', '5GB']:
                k = '8MM'
            label = x[2].strip('"').replace("\\n", "")
            kklist = [kk for kk in TT[k].keys() if type(kk) == int]
            for kk in kklist:
                stlist = TT[k][kk]['list']
                if any(t in s for s in stlist):
                    TT[k][kk]['label'] = label
                    break
    msg.close()


# -----------------------------------------------------------------------------
def ymdhms(epoch):
    return time.strftime("%Y.%m%d %H:%M:%S",
                         time.localtime(epoch))

# -----------------------------------------------------------------------------
# toolframe.tf_launch(prefix, __name__)
