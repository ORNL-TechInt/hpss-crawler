import Checkable
import crawl_lib
import CrawlConfig
import CrawlDBI
import cv_lib
import dbschem
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
import util as U

prefix = "cvv"
H = None


# -----------------------------------------------------------------------------
def cvv_addcart(argv):
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

    result = dbschem.alter_table(table="checkables",
                                 addcol="cart text",
                                 pos="after cos")
    print(result)


# -----------------------------------------------------------------------------
def cvv_dropcart(argv):
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

    result = dbschem.alter_table(table="checkables", dropcol="cart")
    print(result)


# -----------------------------------------------------------------------------
def cvv_popcart(argv):
    """popcart - Populate field cart in checkable table

    usage: cv popcart [-d] [-n] [-s] [-l N] [path ...]

    For each file path in the mysql database that doesn't already have a cart
    name, issue an 'ls -P <path>' in hsi to retrieve the name of the cartridge
    where the file is stored. Record the cartridge name in the database.

    If path name(s) are specified on the command line, only the records for
    those paths will be updated with a cart name.
    """
    p = optparse.OptionParser()
    p.add_option('-c', '--check',
                 action='store_true', default=False, dest='check',
                 help='only records that have been checked')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-l', '--limit',
                 action='store', default=0, dest='limit', type=int,
                 help='stop after doing this many records')
    p.add_option('-n', '--dry-run',
                 action='store_true', default=False, dest='dryrun',
                 help='show which rows would be updated')
    p.add_option('-s', '--skip',
                 action='store_true', default=False, dest='skip',
                 help='skip rows with non-null cart')
    p.add_option('-v', '--verbose',
                 action='store_true', default=False, dest='verbose',
                 help='more info')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    if 0 < len(a):
        b = ["'%s'" % x for x in a]
        where = "path in (" + ','.join(b) + ")"
    else:
        where = "type = 'f'"

    if o.check:
        where += " and last_check <> 0"
    if o.skip:
        where += " and cart is null"

    # get the list of paths and carts from the database
    pc_l = cv_lib.prep_popcart(where, o.limit)

    # generate an updated list from hsi
    upc_l = populate_cart_field(pc_l, o.limit, o.dryrun, o.verbose)

    if o.dryrun:
        # report what would have been changed
        for (p, d, h) in upc_l:
            print("would change '%s' to '%s' for path %s" % (d, h, p))
    else:
        # now update the database with the collected info
        cv_lib.popcart(upc_l)


# -----------------------------------------------------------------------------
def populate_cart_field(pc_l, limit, dryrun, verbose):
    """
    We get a list of paths and carts. The cart values may be empty or None. We
    talk to hsi to collect cart info for each of the paths, building a return
    list. If 0 < limit, the list returned is limit elements long. If dryrun, we
    just report what would happen without actually doing anything.

    In the array of tuples returned, the cart value comes first so we can pass
    the list to a db.update() call that is going to match on path.
    """
    h = hpss.HSI(verbose=True)
    rval = []
    for path, dcart in pc_l:
        info = h.lsP(path)
        hcart = info.split("\t")[5].strip()
        if dcart != hcart:
            if 0 < limit:
                try:
                    populate_cart_field._count += 1
                except AttributeError:
                    populate_cart_field._count = 1
                if 0 < limit and limit < populate_cart_field._count:
                    return True
            rval.append((path, dcart, hcart))
        if verbose:
            if 60 < len(path):
                dpath = '...' + path[-57:]
            else:
                dpath = path

            print("%-60s %-8s %-10s" % (dpath, dcart, hcart))

    h.quit()
    return rval


# -----------------------------------------------------------------------------
def cvv_report(argv):
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
    dim['ttypes'] = Dimension.get_dim('ttypes')

    print dim['cos'].report()
    print dim['ttypes'].report()


# -----------------------------------------------------------------------------
def cvv_nulltest(argv):
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

    rows = cv_lib.nulls_from_checkables()
    hfmt = "%5s %-45s %4s %-4s %-8s %20s %3s %11s %3s %3s"
    rfmt = "%5d %-47.47s %-2s %4s %-8s %20s  %2d  %11d %2d  %2d"
    headers = ("Rowid", "Path", "Type", "COS", "Cart", "TType", "Chk",
               "Last Check", "Fls", "Rpt")
    print(hfmt % headers)
    rcount = 0
    for r in rows:
        print(rfmt % r)
        rcount += 1
        if 50 < rcount:
            print(hfmt % headers)
            rcount = 0


# -----------------------------------------------------------------------------
def cvv_fail_reset(argv):
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

    cv_lib.reset_path(o.pathname)


# -----------------------------------------------------------------------------
def cvv_show_next(argv):
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
            print("%s %s %s" % (U.ymdhms(c.last_check),
                                c.type,
                                c.path))


# -----------------------------------------------------------------------------
def cvv_simplug(argv):
    """simplug - Simulate running a plugin

    usage: cv simplug [-d] [-i <iterations>]

    Simulate running the checksum-verifier plugin
    """
    crawl_lib.simplug('cv', argv)


# -----------------------------------------------------------------------------
def cvv_test_check(argv):
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
def cvv_ttype_add(argv):
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

    result = dbschem.alter_table(table="checkables",
                                 addcol="ttypes text",
                                 pos="after cart")
    print(result)


# -----------------------------------------------------------------------------
def cvv_ttype_drop(argv):
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

    result = dbschem.alter_table(table="checkables", dropcol="ttypes")
    print(result)


# -----------------------------------------------------------------------------
def cvv_ttype_lookup(argv):
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

    rpt = {}
    for path in a:
        media = cv_lib.ttype_lookup(path)
        rpt[path] = media

    pwid = max([len(x) for x in rpt.keys()])
    for path in rpt.keys():
        for (c, m) in rpt[path]:
            print("%*s %s %s" % (-pwid, path, c, m))
# -----------------------------------------------------------------------------
def cvv_ttype_missing(argv):
    """ttype_missing - Report records missing ttype information

    usage: cv ttype_missing [-d]
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-c', '--config',
                 action='store', default='', dest='config',
                 help='configuration to use')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    CrawlConfig.get_config(o.config)
    rec_l = cv_lib.ttype_missing()
    for rec in rec_l:
        print("%-40s %-10s %s %s" % (rec[1],
                                     rec[4],
                                     rec[5],
                                     U.ymdhms(int(rec[7]))))


# -----------------------------------------------------------------------------
def cvv_ttype_populate(argv):
    """ttype_populate - populate the ttype field in checkables

    usage: cv ttype_populate [-d] [-n] [-l N] [path ... | -a]

    If one or more paths are specified, those records will be looked up in
    checkables and populated with media type information if present. If no
    paths are specified and -a/--all is, all records in checkables will be
    updated with media type info.
    """
    # -------------------------------------------------------------------------
    def report_updated_row_list(row_l):
        if 1 < len(row_l):
            print("Duplicate rows for path %s:" % row_l[0][0])
            for row in row_l:
                report_row(row)
        else:
            report_row(row_l[0])

    # -------------------------------------------------------------------------
    def show_would_do(fields, data):
        ddx = 0
        zs = "\nwould set "
        sep = ""
        for f in fields:
            zs += sep + "%s = %s" % (f, data[ddx])
            ddx += 1
            sep = ", "
        zs += "\n   for path %s" % data[ddx]
        print(zs)

    # -------------------------------------------------------------------------
    def verify_update(path):
        rows = db.select(table="checkables",
                         fields=["path", "cart", "ttypes", "last_check"],
                         where="path = ?",
                         data=(path,))
        report_updated_row_list(rows)

    p = optparse.OptionParser()
    p.add_option('-a', '--all',
                 action='store_true', default=False, dest='all',
                 help='process all records in checkables')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-l', '--limit',
                 action='store', default=-1, dest='limit', type=int,
                 help='max records to process')
    p.add_option('-n', '--dryrun',
                 action='store_true', default=False, dest='dryrun',
                 help='see what would happen')
    p.add_option('-v', '--verbose',
                 action='store_true', default=False, dest='verbose',
                 help='more details')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    # o.all (-a) => consider all paths
    # pathlist   => consider just the paths specified
    # both is an error
    # neither is an error
    if o.all and a:
        raise SystemExit("You can give -a or a list of paths but not both")
    elif o.all:
        candlist = cv_lib.tpop_select_all()
    elif a:
        candlist = cv_lib.tpop_select_by_paths(a)
    else:
        raise SystemExit("Either -a or a list of paths is required")

    # if no paths matched the criteria, tell the user and bail
    if len(candlist) == 0:
        raise SystemExit("No rows found to be populated")

    # okay, we have a list of candidates to work on. Take them one at a time. A
    # single file can be spread across multiple tapes so cv_lib.ttype_lookup
    # will return a list of cart, media type tuples.
    scount = pcount = 0
    data = []
    for row in candlist:
        (path, type, ttype, cart, last_check) = row
        if o.verbose:
            print("%-60s %s %-5s %-10s %d" % row)

        # get a list of cartnames and media type descriptions
        cml = cv_lib.ttype_lookup(path, cart)
        if cml is None:
            print("No cart/media type found for %s" % path)
            scount += 1
            continue

        # make a comma-separated list of cart names from cml
        cartnames = ",".join([x[0] for x in cml])

        # make a comma-separated list of media descriptions from cml
        mdescs = ",".join([x[1] for x in cml])

        # collect the update info
        data.append((mdescs, cartnames, path, last_check))

        pcount += 1
        o.limit -= 1
        if o.limit == 0:
            break

    if o.dryrun:
        show_would_do(data)
    else:
        cv_lib.tpop_update_by_path(data)
        cv_lib.tpop_report_updates(data)
    print("%d records processed, %d records skipped" % (pcount, scount))


# -----------------------------------------------------------------------------
def cvv_ttype_table(argv):
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

    # lookup and report tape type for each pathname specified
    if o.drop:
        result = dbschem.drop_table(table="tape_types")
        print result
    else:
        dbschem.make_table("tape_types")

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
        except KeyError as e:
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

    cv_lib.ttype_map_insert(TT)
