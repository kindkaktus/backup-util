import glob
import os
import datetime
import sys
import subprocess
from boto import config
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import tempfile
import shutil

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate

MAX_ARCHIVE_AGE_DAYS = 60
IS_PYTHON2 = sys.version_info < (3, 0)


def _write_log(log_file, msg):
    f = open(log_file, 'a')
    f.write('[%s] %s\n' % (datetime.datetime.today(), msg))
    f.close()


def _to_utf8(s):
    if IS_PYTHON2:
        if isinstance(s, unicode):
            return s.encode('utf-8')
        else:
            # just suppose it is already utf8. @todo proper implementation should have
            # detected the encoding and convert to utf8 then...
            return s
    else:
        return s.encode('utf-8')


def _to_unicode(s):
    """
    When s is a sequence type it gets converted a string/bytearray
    with elements separated by one space
    """
    if isinstance(s, list) or isinstance(s, tuple):
        if IS_PYTHON2:
            s = " ".join(s)
        else:
            s = b" ".join(s)

    needs_decode = False
    if IS_PYTHON2 and not isinstance(s, unicode):
        needs_decode = True
    if not IS_PYTHON2 and not isinstance(s, str):
        needs_decode = True

    if needs_decode:
        try:
            s = s.decode('utf-8')
        except UnicodeDecodeError as e:
            _write_log(
                "Failed to utf8 decode process output, 'bad' characters will be replaced with U+FFFD. %s." % (str(e)))
            s = s.decode('utf-8', 'replace')
    return s


def _get_log_tail(log_file):
    if not log_file:
        return ""

    try:
        p = subprocess.Popen("tail -n 100 " + log_file, shell=True, stdout=subprocess.PIPE)
        myStdout, _ = p.communicate()
        myStdout = _to_unicode(myStdout)
        if p.returncode == 0:
            return myStdout.rstrip()
        else:
            return ""
    except:
        return ""


def _format_time_delta(aTimeDelta):
    """Return string representation of time delta as 'x day(s), y hour(s), z min, s sec' for pretty-printing.

    param delta the instance of datetime.timedelta
    """
    mySeconds = aTimeDelta.seconds % 60
    myMinutes = (aTimeDelta.seconds // 60) % 60
    myHours = (aTimeDelta.seconds // 3600) % 24
    myDays = aTimeDelta.days
    if myDays != 0:
        return "%u day(s), %u hour(s), %u min, %u sec" % (myDays, myHours, myMinutes, mySeconds)
    if myHours != 0:
        return "%u hour(s), %u min, %u sec" % (myHours, myMinutes, mySeconds)
    if myMinutes != 0:
        return "%u min, %u sec" % (myMinutes, mySeconds)
    return "%u sec" % mySeconds


def _pretty_filesize(filename):
    num = os.path.getsize(filename)
    for x in ['bytes', 'KB', 'MB', 'GB']:
        if num < 1024.0 and num > -1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')


def _svn_backup(svn_dir, backup_dir):
    myCmd = "svnadmin hotcopy --clean-logs %s %s" % (svn_dir, backup_dir)
    myProcess = subprocess.Popen(
        myCmd, shell=True, cwd=svn_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    myStdout, myStderr = myProcess.communicate()
    myStdout = _to_unicode(myStdout)
    myStderr = _to_unicode(myStderr)

    if myProcess.returncode != 0:
        return {"ret": False,
                "description": "svn backup at %s finished with return code %d." % (svn_dir, myProcess.returncode),
                "stdout": myStdout.rstrip(),
                "stderr": myStderr.rstrip()}
    return {"ret": True,
            "description": "svn backup at %s completed successfully." % svn_dir,
            "stdout": myStdout.rstrip(),
            "stderr": myStderr.rstrip()}


def _svn_update(svn_dir):
    myCmd = "svn up --non-interactive --trust-server-cert"
    myProcess = subprocess.Popen(
        myCmd, shell=True, cwd=svn_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    myStdout, myStderr = myProcess.communicate()
    myStdout = _to_unicode(myStdout)
    myStderr = _to_unicode(myStderr)

    if myProcess.returncode != 0:
        return {"ret": False,
                "description": "'svn update' of %s finished with return code %d." % (svn_dir, myProcess.returncode),
                "stdout": myStdout.rstrip(),
                "stderr": myStderr.rstrip()}
    return {"ret": True,
            "description": "'svn update' of %s completed successfully." % svn_dir,
            "stdout": myStdout.rstrip(),
            "stderr": myStderr.rstrip()}


def _git_backup(clone_url, repo_archive_path):
    temp_dir = tempfile.mkdtemp()
    myCmd = "git clone %s ./ && git bundle create %s --all" % (clone_url, repo_archive_path)
    myProcess = subprocess.Popen(
        myCmd, shell=True, cwd=temp_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    myStdout, myStderr = myProcess.communicate()
    shutil.rmtree(temp_dir, ignore_errors=True)
    myStdout = _to_unicode(myStdout)
    myStderr = _to_unicode(myStderr)

    if myProcess.returncode != 0:
        return {"ret": False,
                "description": "git local backup from %s to %s finished with return code %d." % (clone_url, repo_archive_path, myProcess.returncode),
                "stdout": myStdout.rstrip(),
                "stderr": myStderr.rstrip()}
    return {"ret": True,
            "description": "git local backup from %s to %s completed successfully." % (clone_url, repo_archive_path),
            "stdout": myStdout.rstrip(),
            "stderr": myStderr.rstrip()}


def _trac_backup(trac_dir, backup_dir):
    myCmd = "/usr/local/bin/trac-admin %s hotcopy %s" % (trac_dir, backup_dir)
    myProcess = subprocess.Popen(
        myCmd, shell=True, cwd=trac_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    myStdout, myStderr = myProcess.communicate()
    myStdout = _to_unicode(myStdout)
    myStderr = _to_unicode(myStderr)

    if myProcess.returncode != 0:
        return {"ret": False,
                "description": "svn backup at %s finished with return code %d." % (trac_dir, myProcess.returncode),
                "stdout": myStdout.rstrip(),
                "stderr": myStderr.rstrip()}
    return {"ret": True,
            "description": "svn backup at %s completed successfully." % trac_dir,
            "stdout": myStdout.rstrip(),
            "stderr": myStderr.rstrip()}


def _archive(src_dir, dest_archive):
    archive_password = config.get('Credentials', 's3_backup_passphrase')
    myCmd = "7za a -t7z -mhe=on -p%s %s *" % (archive_password, dest_archive)
    if not os.path.exists(os.path.dirname(dest_archive)):
        os.makedirs(os.path.dirname(dest_archive))
    with open(os.devnull, "w") as fnull:
        # skip stdout since it contains all files added to the archive
        myProcess = subprocess.Popen(
            myCmd, shell=True, cwd=src_dir, stdout=fnull, stderr=subprocess.PIPE)
        _, myStderr = myProcess.communicate()
        myStderr = _to_unicode(myStderr)

        if myProcess.returncode != 0:
            return {"ret": False,
                    "description": "archiving %s to %s finished with return code %d." % (src_dir, dest_archive, myProcess.returncode),
                    "stderr": myStderr.rstrip()}
        return {"ret": True,
                "description": "archiving %s to %s completed successfully." % (src_dir, dest_archive),
                "stderr": myStderr.rstrip()}


def _cleanup_old_archines(dir, extension, max_age=MAX_ARCHIVE_AGE_DAYS):
    myCmd = "find %s/*%s -mtime +%d -exec rm -f {} \;" % (dir, extension, max_age)
    with open(os.devnull, "w") as fnull:
        myProcess = subprocess.Popen(
            myCmd, shell=True, cwd=dir, stdout=fnull, stderr=subprocess.PIPE)
        _, myStderr = myProcess.communicate()
        myStderr = _to_unicode(myStderr)

        if myProcess.returncode != 0:
            return {"ret": False,
                    "description": "cleaning up %s/*%s finished with return code %d." % (dir, extension, myProcess.returncode),
                    "stderr": myStderr.rstrip()}
        return {"ret": True,
                "description": "cleaning up %s/*%s completed successfully." % (dir, extension),
                "stderr": myStderr.rstrip()}


def send_email(aSubj, aMsg, aSender, aRecepients, aLogFile, anSmtpSvrHost='localhost', anSmtpSvrPort=25, aUser=None, aPassword=None):

    log_tail = _get_log_tail(aLogFile)
    msg = MIMEMultipart()
    attachment = MIMEText(_to_utf8(log_tail), 'plain', 'utf-8')
    attachment.add_header('Content-Disposition', 'attachment', filename=aLogFile)
    msg.attach(attachment)
    msg.attach(MIMEText(_to_utf8(aMsg), 'plain', 'utf-8'))

    msg['Subject'] = aSubj
    msg['From'] = aSender
    msg['To'] = ', '.join(aRecepients)
    msg['Date'] = formatdate()

    mySmtpSvr = smtplib.SMTP(anSmtpSvrHost, anSmtpSvrPort)
    if aUser and aPassword is not None:
        mySmtpSvr.login(aUser, aPassword)

    # mySmtpSvr.set_debuglevel(1)
    mySmtpSvr.sendmail(aSender, aRecepients, msg.as_string())
    mySmtpSvr.quit()


def backup_dir(hint, dir, archive_path, bucket_name, log_file):
    status_brief = '[S3 Backup] %s' % hint
    status_detailed = ''
    backupOk = False
    start = datetime.datetime.today()
    _write_log(log_file, 'Starting backup')
    try:
        _write_log(log_file, "Archiving %s to %s" % (dir, archive_path))
        ret = _archive(dir, archive_path)
        _write_log(log_file, '%s\nStdErr: %s\n' % (ret['description'], ret['stderr']))
        if ret['ret']:
            conn = S3Connection()
            bucket = conn.get_bucket(bucket_name)
            k = Key(bucket)
            status_detailed = 'Uploading %s (%s) to S3...' % (
                archive_path, _pretty_filesize(archive_path))
            k.key = os.path.basename(archive_path)
            k.set_contents_from_filename(archive_path)
            backupOk = True
            status_detailed += 'done.'
    except BaseException as e:
        status_detailed += '\nError: %s. %s' % (type(e), str(e))
    except:
        status_detailed += '\nUnknown error'
    finally:
        if backupOk:
            status_brief += ' OK'
            ret = _cleanup_old_archines(dir=os.path.dirname(archive_path),
                                        extension=os.path.splitext(archive_path)[1])
            _write_log(log_file, '%s\nStdErr: %s\n' % (ret['description'], ret['stderr']))
        else:
            status_brief += ' FAILED'
        end = datetime.datetime.today()
        status_detailed += '\nElapsed time: %s' % _format_time_delta(end - start)
        _write_log(log_file, status_detailed)
        _write_log(log_file, 'Backup to %s finished with status %s' %
                   (bucket_name, 'SUCCESS' if backupOk else 'ERROR'))
        return {'retval': backupOk, 'status_brief': status_brief, 'status_detailed': status_detailed}


class SvnBackupType:
    REPO = 1
    WORKING_COPY = 2


def _backup_svn(backup_name_hint, svn_backup_type, svn_url, archive_path, bucket_name, log_file):
    status_brief = '[S3 Backup] %s' % backup_name_hint
    status_detailed = ''
    backupOk = False
    start = datetime.datetime.today()
    _write_log(log_file, 'Starting backup')
    svn_backup_dir = None
    try:
        if svn_backup_type == SvnBackupType.REPO:
            _write_log(log_file, "Backing up svn repo at %s" % svn_url)
            svn_backup_dir = tempfile.mkdtemp()
            ret = _svn_backup(svn_url, svn_backup_dir)
            svn_url = svn_backup_dir
        elif svn_backup_type == SvnBackupType.WORKING_COPY:
            _write_log(log_file, "Updating %s" % svn_url)
            ret = _svn_update(svn_url)
        else:
            raise Exception("Unsupported svn backup type %s" % svn_backup_type)
        _write_log(log_file, '%s\nStdOut: %s\nStdErr: %s\n' %
                   (ret['description'], ret['stdout'], ret['stderr']))
        if ret['ret']:
            _write_log(log_file, "Archiving %s to %s" % (svn_url, archive_path))
            ret = _archive(svn_url, archive_path)
            _write_log(log_file, '%s\nStdErr: %s\n' % (ret['description'], ret['stderr']))
            if ret['ret']:
                conn = S3Connection()
                bucket = conn.get_bucket(bucket_name)
                k = Key(bucket)
                status_detailed = 'Uploading %s (%s) to S3...' % (
                    archive_path, _pretty_filesize(archive_path))
                k.key = os.path.basename(archive_path)
                k.set_contents_from_filename(archive_path)
                backupOk = True
                status_detailed += 'done.'
    except BaseException as e:
        status_detailed += '\nError: %s. %s' % (type(e), str(e))
    except:
        status_detailed += '\nUnknown error'
    finally:
        if svn_backup_dir is not None:
            shutil.rmtree(svn_backup_dir, ignore_errors=True)
        if backupOk:
            status_brief += ' OK'
            ret = _cleanup_old_archines(dir=os.path.dirname(archive_path),
                                        extension=os.path.splitext(archive_path)[1])
            _write_log(log_file, '%s\nStdErr: %s\n' % (ret['description'], ret['stderr']))
        else:
            status_brief += ' FAILED'
        end = datetime.datetime.today()
        status_detailed += '\nElapsed time: %s' % _format_time_delta(end - start)
        _write_log(log_file, status_detailed)
        _write_log(log_file, 'Backup to %s finished with status %s' %
                   (bucket_name, 'SUCCESS' if backupOk else 'ERROR'))
        return {'retval': backupOk, 'status_brief': status_brief, 'status_detailed': status_detailed}


def backup_svn_repo(backup_name_hint, svn_url, archive_path, bucket_name, log_file):
    return _backup_svn(backup_name_hint, SvnBackupType.REPO, svn_url, archive_path, bucket_name, log_file)


def backup_svn_wc(backup_name_hint, svn_dir, archive_path, bucket_name, log_file):
    return _backup_svn(backup_name_hint, SvnBackupType.WORKING_COPY, svn_dir, archive_path, bucket_name, log_file)


def backup_git_repo(backup_name_hint, clone_url, archive_path, bucket_name, log_file):
    status_brief = '[S3 Backup] %s' % backup_name_hint
    status_detailed = ''
    backupOk = False
    start = datetime.datetime.today()
    _write_log(log_file, 'Starting backup')
    temp_backup_dir = None
    try:
        _write_log(log_file, "Backing up git repo at %s" % archive_path)
        temp_backup_dir = tempfile.mkdtemp()
        ret = _git_backup(clone_url, temp_backup_dir + '/git_repo.bundle')
        _write_log(log_file, '%s\nStdOut: %s\nStdErr: %s\n' %
                   (ret['description'], ret['stdout'], ret['stderr']))
        if ret['ret']:
            _write_log(log_file, "Archiving %s to %s" % (temp_backup_dir, archive_path))
            ret = _archive(temp_backup_dir, archive_path)
            _write_log(log_file, '%s\nStdErr: %s\n' % (ret['description'], ret['stderr']))
            if ret['ret']:
                conn = S3Connection()
                bucket = conn.get_bucket(bucket_name)
                k = Key(bucket)
                status_detailed = 'Uploading %s (%s) to S3...' % (
                    archive_path, _pretty_filesize(archive_path))
                k.key = os.path.basename(archive_path)
                k.set_contents_from_filename(archive_path)
                backupOk = True
                status_detailed += 'done.'
    except BaseException as e:
        status_detailed += '\nError: %s. %s' % (type(e), str(e))
    except:
        status_detailed += '\nUnknown error'
    finally:
        if temp_backup_dir is not None:
            shutil.rmtree(temp_backup_dir, ignore_errors=True)
        if backupOk:
            status_brief += ' OK'
            ret = _cleanup_old_archines(dir=os.path.dirname(archive_path),
                                        extension=os.path.splitext(archive_path)[1])
            _write_log(log_file, '%s\nStdErr: %s\n' % (ret['description'], ret['stderr']))
        else:
            status_brief += ' FAILED'
        end = datetime.datetime.today()
        status_detailed += '\nElapsed time: %s' % _format_time_delta(end - start)
        _write_log(log_file, status_detailed)
        _write_log(log_file, 'Backup to %s finished with status %s' %
                   (bucket_name, 'SUCCESS' if backupOk else 'ERROR'))
        return {'retval': backupOk, 'status_brief': status_brief, 'status_detailed': status_detailed}


def backup_latest(backup_name_hint, backup_filemask, bucket_name, log_file):
    status_brief = '[S3 Backup] %s' % backup_name_hint
    status_detailed = ''
    backupOk = False
    start = datetime.datetime.today()
    _write_log(log_file, 'Starting backup %s to %s' % (backup_name_hint, bucket_name))
    try:
        files = sorted(glob.glob(backup_filemask), key=lambda filename: os.stat(filename).st_mtime)
        if len(files) == 0:
            backupOk = True
            status_detailed = 'Nothing to backup in %s' % backup_filemask
        else:
            backup_filepath = files[-1]
            conn = S3Connection()
            bucket = conn.get_bucket(bucket_name)
            k = Key(bucket)
            status_detailed = 'Uploading %s to S3...' % backup_filepath
            _write_log(log_file, 'Uploading %s (%s) to S3...' %
                       (backup_filepath, _pretty_filesize(backup_filepath)))
            k.key = os.path.basename(backup_filepath)
            k.set_contents_from_filename(backup_filepath)
            backupOk = True
            status_detailed += 'done.'
    except BaseException as e:
        status_detailed += '\nError: %s. %s' % (type(e), str(e))
    except:
        status_detailed += '\nUnknown error'
    finally:
        if backupOk:
            status_brief += ' OK'
        else:
            status_brief += ' FAILED'
        end = datetime.datetime.today()
        status_detailed += '\nElapsed time: %s' % _format_time_delta(end - start)
        _write_log(log_file, status_detailed)
        _write_log(log_file, 'Backup to %s finished with status %s' %
                   (bucket_name, 'SUCCESS' if backupOk else 'ERROR'))
        return {'retval': backupOk, 'status_brief': status_brief, 'status_detailed': status_detailed}


def backup_trac(backup_name_hint, trac_dir, archive_path, bucket_name, log_file):
    status_brief = '[S3 Backup] %s' % backup_name_hint
    status_detailed = ''
    backupOk = False
    start = datetime.datetime.today()
    _write_log(log_file, 'Starting backup')
    temp_backup_dir = None
    try:
        _write_log(log_file, "Backing up TRAC at %s" % trac_dir)
        temp_backup_dir = tempfile.mkdtemp() + "/trac"
        ret = _trac_backup(trac_dir, temp_backup_dir)
        _write_log(log_file, '%s\nStdOut: %s\nStdErr: %s\n' %
                   (ret['description'], ret['stdout'], ret['stderr']))
        if ret['ret']:
            _write_log(log_file, "Archiving %s to %s" % (temp_backup_dir, archive_path))
            ret = _archive(temp_backup_dir, archive_path)
            _write_log(log_file, '%s\nStdErr: %s\n' % (ret['description'], ret['stderr']))
            if ret['ret']:
                conn = S3Connection()
                bucket = conn.get_bucket(bucket_name)
                k = Key(bucket)
                status_detailed = 'Uploading %s (%s) to S3...' % (
                    archive_path, _pretty_filesize(archive_path))
                k.key = os.path.basename(archive_path)
                k.set_contents_from_filename(archive_path)
                backupOk = True
                status_detailed += 'done.'
    except BaseException as e:
        status_detailed += '\nError: %s. %s' % (type(e), str(e))
    except:
        status_detailed += '\nUnknown error'
    finally:
        if temp_backup_dir is not None:
            shutil.rmtree(temp_backup_dir, ignore_errors=True)
        if backupOk:
            status_brief += ' OK'
            ret = _cleanup_old_archines(dir=os.path.dirname(archive_path),
                                        extension=os.path.splitext(archive_path)[1])
            _write_log(log_file, '%s\nStdErr: %s\n' % (ret['description'], ret['stderr']))
        else:
            status_brief += ' FAILED'
        end = datetime.datetime.today()
        status_detailed += '\nElapsed time: %s' % _format_time_delta(end - start)
        _write_log(log_file, status_detailed)
        _write_log(log_file, 'Backup to %s finished with status %s' %
                   (bucket_name, 'SUCCESS' if backupOk else 'ERROR'))
        return {'retval': backupOk, 'status_brief': status_brief, 'status_detailed': status_detailed}


def download_latest(bucket_name, file_prefix, store_dir, log_file):
    status_brief = '[S3 Backup] Get the latest backup starting with %s from %s:' % (
        file_prefix, bucket_name)
    status_detailed = ''
    downloadOk = False
    start = datetime.datetime.today()
    _write_log(log_file, 'Starting download')
    try:
        _write_log(log_file, '[S3 backup] Downloading the latest backup starting with %s from bucket %s to %s' % (
            file_prefix, bucket_name, store_dir))
        conn = S3Connection()
        bucket = conn.get_bucket(bucket_name)
        if bucket:
            if file_prefix:
                key_list = [k for k in bucket if k.name.startswith(file_prefix)]
            else:
                key_list = [k for k in bucket]
            if key_list:
                key_list.sort(key=lambda k: k.last_modified)
                key_to_download = key_list[-1]
                store_path = os.path.join(store_dir, key_to_download.name)
                status_detailed = 'Downloading %s from %s to %s...' % (
                    key_to_download.name, bucket_name, store_path)
                key_to_download.get_contents_to_filename(store_path)
                downloadOk = True
            else:
                status_detailed = 'No file found in bucket %s starting with %s, nothing to do\n' % (
                    bucket_name, file_prefix)

        status_detailed += 'done.'
    except BaseException as e:
        status_detailed += '\nError: %s. %s' % (type(e), str(e))
    except:
        status_detailed += '\nUnknown error'
    finally:
        if downloadOk:
            status_brief += ' OK'
        else:
            status_brief += ' FAILED'
        end = datetime.datetime.today()
        status_detailed += '\nElapsed time: %s' % _format_time_delta(end - start)
        _write_log(log_file, status_detailed)
        _write_log(log_file, 'Download from %s to %s finished with status %s' %
                   (bucket_name, store_dir, 'SUCCESS' if downloadOk else 'ERROR'))
        return {'retval': downloadOk, 'status_brief': status_brief, 'status_detailed': status_detailed}
