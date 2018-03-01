
from __future__ import absolute_import

import logging
import os
from time import sleep

try:
    from ConfigParser import ConfigParser
except:
    from configparser import ConfigParser


LOG = logging.getLogger(__name__)


def cat(fns, outfn):
    """
    cat files together
    NOT USED NOW
    :param fns:
    :param outfn:
    :return:
    """
    LOG.debug("cat %s >%s" % (" ".join(fns), outfn))
    with open(outfn, "w") as out:
        for fn in fns:
            out.write(open(fn).read())

    return outfn


def cd(newdir):
    """
    from FALCON_KIT
    :param newdir:
    :return:
    """
    newdir = os.path.abspath(newdir)
    prevdir = os.getcwd()
    LOG.debug('CD: %r <- %r' % (newdir, prevdir))
    os.chdir(os.path.expanduser(newdir))
    return newdir


def check_paths(*paths):
    """
    check the existence of paths
    :param paths:
    :return: abs paths
    """
    r = []
    for path in paths:
        path = os.path.abspath(path)
        if not os.path.exists(path):
            msg = "File not found '{path}'".format(**locals())
            LOG.error(msg)
            raise Exception(msg)
        r.append(path)
    if len(r) == 1:
        return r[0]
    return r


def check_status(fns, sleep_time):
    """
    check the existence of a list of done file until all done
    NOT USED
    :param fns:
    :param sleep_time:
    :return:
    """
    while 1:
        LOG.info("sleep %s" % sleep_time)
        sleep(sleep_time)
        done_num = 0
        for fn in fns:
            if os.path.exists(fn):
                done_num += 1
        if done_num == len(fns):
            LOG.info("all done")
            break
        else:
            LOG.info("%s done, %s running" % (done_num, len(fns) - done_num))

    return 1


def link(source, target, force=False):
    """
    link -s
    :param source:
    :param target:
    :param force:
    :return:
    """
    source = check_paths(source)

    # for link -sf
    if os.path.isfile(target):
        if force:
            os.remove(target)

    LOG.info("ln -s {source} {target}".format(**locals()))
    os.symlink(source, target)

    return target


def mkdir(d):
    """
    from FALCON_KIT
    :param d:
    :return:
    """
    d = os.path.abspath(d)
    if not os.path.isdir(d):
        LOG.debug('mkdir {!r}'.format(d))
        os.makedirs(d)
    else:
        LOG.debug('mkdir {!r}, {!r} exist'.format(d, d))

    return d


def touch(*paths):
    """
    touch a file.
    from FALCON_KIT
    """

    for path in paths:
        if os.path.exists(path):
            os.utime(path, None)
        else:
            open(path, 'a').close()
            LOG.debug('touch {!r}'.format(path))


def str2dict(string):
    """
    transform string "-a b " or "--a b" to dict {"a": "b"}
    :param string:
    :return:
    """
    assert isinstance(string, str)
    r = {}

    param = ""
    value = []
    for p in string.split():

        if not p:
            continue

        if p.startswith("-"):
            if param:
                if value:
                    r[param] = " ".join(value)
                else:
                    r[param] = True

                param = p.lstrip("-")
                value = []
        else:
            value.append(p)

    return r


def read_config(cfg):
    """
    read config fron ini
    :param cfg:
    :return:
    """
    check_paths(cfg)

    r = {}
    config = ConfigParser()
    config.read(cfg)

    for section in config.sections():
        r[section] = {}

        for option in config.options(section):
            value = config.get(section, option).strip()
            r[section][option] = value

    return r

