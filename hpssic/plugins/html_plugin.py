from hpssic import CrawlConfig
from hpssic import html_lib
import os


# -----------------------------------------------------------------------------
def main(cfg):
    """
    Generate an html-formatted report and store it at the designated location
    """
    CrawlConfig.log("html_plugin starting")
    fpath = cfg.get('html', 'output_path')
    rpt = html_lib.get_html_report(cfg=cfg)

    npath = fpath + '.new'
    opath = fpath + '.old'
    with open(npath, 'w') as out:
        out.write(rpt)

    if os.path.exists(fpath):
        os.rename(fpath, opath)
    os.rename(npath, fpath)
    CrawlConfig.log("html_plugin finished")
