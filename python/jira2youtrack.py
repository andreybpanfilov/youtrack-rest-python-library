#! /usr/bin/env python

# **********************************************************
# *  Since YouTrack 6.5 there is build-in JIRA import.     *
# *  Please use this script only in case you have problems *
# *  with the native implementation.                       *
# **********************************************************

import calendar
import functools
import sys

reload(sys)
sys.setdefaultencoding('utf8')
import os
import re
import getopt
import datetime
import urllib2
import jira
from jira.client import JiraClient
from youtrack import YouTrackException, Link, WorkItem
import youtrack
from youtrack.connection import Connection
from youtrack.importHelper import create_bundle_safe

jt_fields = []

_debug = os.environ.get('DEBUG')


def usage():
    print """
**********************************************************
*  Since YouTrack 6.5 there is build-in JIRA import.     *
*  Please use this script only in case you have problems *
*  with the native implementation.                       *
**********************************************************

Usage:
    %s [OPTIONS] j_url j_user j_pass y_url y_user y_pass [project_id[,range] ...]

The script imports issues from Jira to YouTrack.
By default it imports issues and all attributes like attachments, labels, links.
This behaviour can be changed by passing import options -i, -a, -l, -t amd -w.

Arguments:
    j_url         Jira URL
    j_user        Jira user
    j_pass        Jira user's password
    y_url         YouTrack URL
    y_user        YouTrack user
    y_pass        YouTrack user's password
    project_id    ProjectID to import
    range         Import issues from given range only. Format is [X:]Y.
                  Default value for X is 1, so it can be omitted.
                  Examples: DEMO,100, DEMO,101:200

Options:
    -h,  Show this help and exit
    -b,  Batch size
    -i,  Import issues
    -a,  Import attachments
    -r,  Replace old attachments with new ones (remove and re-import)
    -l,  Import issue links
    -t,  Import Jira labels (convert to YT tags)
    -w,  Import Jira work logs
    -m,  Comma-separated list of field mappings.
         Mapping format is JIRA_FIELD_NAME:YT_FIELD_NAME@FIELD_TYPE
    -M,  Comma-separated list of field value mappings.
         Mapping format is YT_FIELD_NAME:JIRA_FIELD_VALUE=YT_FIELD_VALUE[;...]
    -D,  Comma-separated list of fields which compose description
    -S,  Comma-separated list of fields to skip
""" % os.path.basename(sys.argv[0])


# Primary import options
FI_ISSUES = 0x01
FI_ATTACHMENTS = 0x02
FI_LINKS = 0x04
FI_LABELS = 0x08
FI_WORK_LOG = 0x16

# Secondary import options (from 0x80)
FI_REPLACE_ATTACHMENTS = 0x80


def main():
    flags = 0
    field_mappings = dict()
    value_mappings = dict()
    description_fields = []
    skip_fields = []
    batch_size = 10
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'harltiwb:m:M:D:S:')
        for opt, val in opts:
            if opt == '-h':
                usage()
                sys.exit(0)
            elif opt == '-i':
                flags |= FI_ISSUES
            elif opt == '-a':
                flags |= FI_ATTACHMENTS
            elif opt == '-r':
                flags |= FI_REPLACE_ATTACHMENTS
            elif opt == '-l':
                flags |= FI_LINKS
            elif opt == '-t':
                flags |= FI_LABELS
            elif opt == '-w':
                flags |= FI_WORK_LOG
            elif opt == '-m':
                for mapping in val.split(','):
                    m = re.match(r'^([^:]+):([^@]+)@(.+)$', mapping)
                    if not m:
                        raise ValueError('Bad field mapping (skipped): %s' % mapping)
                    jira_name, yt_name, field_type = m.groups()
                    field_mappings[jira_name.lower()] = (yt_name.lower(), field_type)
            elif opt == '-M':
                for mapping in val.split(','):
                    m = re.match(r'^([^:]+):(.+)$', mapping)
                    if not m:
                        raise ValueError('Bad field mapping (skipped): %s' % mapping)
                    field_name, v_mappings = m.groups()
                    field_name = field_name.lower()
                    for vm in v_mappings.split(';'):
                        m = re.match(r'^([^=]+)=(.+)$', vm)
                        if not m:
                            raise ValueError('Bad field mapping (skipped): %s' % vm)
                        jira_value, yt_value = m.groups()
                        if field_name not in value_mappings:
                            value_mappings[field_name] = dict()
                        value_mappings[field_name][jira_value.lower()] = yt_value
            elif opt == '-D':
                description_fields += [f.lower() for f in re.split(",\s*", val)]
            elif opt == '-S':
                skip_fields += [f.lower() for f in re.split(",\s*", val)]
            elif opt == '-b':
                batch_size = int(val)

    except getopt.GetoptError, e:
        print e
        usage()
        sys.exit(1)
    if len(args) < 7:
        print 'Not enough arguments'
        usage()
        sys.exit(1)

    if not flags & 0x7F:
        flags |= FI_ISSUES | FI_ATTACHMENTS | FI_LINKS | FI_LABELS | FI_WORK_LOG
    j_url, j_login, j_password, y_url, y_login, y_password = args[:6]

    projects = []
    for project in args[6:]:
        m = re.match(
            r'^(?P<pid>[^,]+)(?:,(?P<n1>\d+)(?::(?P<n2>\d+))?)?$', project)
        if m:
            m = m.groupdict()
            start = 1
            end = 0
            if m.get('n2') is not None:
                start = int(m['n1'])
                end = int(m['n2'])
            elif m.get('n1') is not None:
                start = 1
                end = int(m['n1'])
            if end and end < start:
                raise ValueError('Bad argument => %s' % project)
            projects.append((m['pid'].upper(), start, end))
        else:
            raise ValueError('Bad argument => %s' % project)

    jira2youtrack(j_url, j_login, j_password,
                  y_url, y_login, y_password, projects,
                  flags, batch_size, field_mappings, value_mappings,
                  description_fields, skip_fields)


def ignore_youtrack_exceptions(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except YouTrackException, e:
            print e

    return wrapper


@ignore_youtrack_exceptions
def process_labels(target, issue):
    tags = issue['fields']['labels']
    for tag in tags:
        tag = re.sub(r'[,&<>]', '_', tag)
        try:
            target.executeCommand(issue['key'], 'tag ' + tag, disable_notifications=True)
        except YouTrackException:
            tag = re.sub(r'[\s-]', '_', tag)
            target.executeCommand(issue['key'], 'tag ' + tag, disable_notifications=True)


def get_yt_field_name(jira_name):
    # case sensitive
    return jira.FIELD_NAMES.get(jira_name, jira_name)


def get_yt_field_type(yt_name):
    # case sensitive
    return jira.FIELD_TYPES.get(yt_name, youtrack.EXISTING_FIELD_TYPES.get(yt_name))


def get_yt_field_value(field_name, jira_value, value_mappings):
    if isinstance(field_name, unicode):
        field_name = field_name.encode('utf-8')
    if isinstance(jira_value, unicode):
        jira_value = jira_value.encode('utf-8')
    # we may have custom priority mapping
    if field_name.lower() in value_mappings:
        return value_mappings[field_name.lower()].get(jira_value.lower(), jira_value)
    # default priority mapping
    elif field_name.lower() == 'priority':
        return jira.PRIORITIES.get(jira_value.lower(), jira_value)
    return jira_value


def process_links(target, issue, yt_links):
    for sub_task in issue['fields']['subtasks']:
        parent = issue[u'key']
        child = sub_task[u'key']
        link = Link()
        link.typeName = u'subtask'
        link.source = parent
        link.target = child
        yt_links.append(link)

    links = issue['fields'][u'issuelinks']
    for link in links:
        if u'inwardIssue' in link:
            source_issue = issue[u'key']
            target_issue = link[u'inwardIssue'][u'key']
        elif u'outwardIssue' in link:
            source_issue = issue[u'key']
            target_issue = link[u'outwardIssue'][u'key']
        else:
            continue

        type = link[u'type']
        type_name = type[u'name']
        inward = type[u'inward']
        outward = type[u'outward']
        try:
            target.createIssueLinkTypeDetailed(
                type_name, outward, inward, inward != outward)
        except YouTrackException, e:
            pass

        yt_link = Link()
        yt_link.typeName = type_name
        yt_link.source = source_issue
        yt_link.target = target_issue
        yt_links.append(yt_link)


def create_user(target, value):
    try:
        name = value['name'].replace(' ', '_')
        target.createUserDetailed(name, value['displayName'], value[u'name'], 'fake_jabber')
    except YouTrackException, e:
        print(str(e))
    except KeyError, e:
        print(str(e))


def to_unix_date(time_string, truncate=False):
    tz_diff = 0
    if len(time_string) == 10:
        dt = datetime.datetime.strptime(time_string, '%Y-%m-%d')
    else:
        m = re.search('(Z|([+-])(\d\d):?(\d\d))$', time_string)
        if m:
            tzm = m.groups()
            time_string = time_string[0:-len(tzm[0])]
            if tzm[0] != 'Z':
                tz_diff = int(tzm[2]) * 60 + int(tzm[3])
                if tzm[1] == '-':
                    tz_diff = -tz_diff
        time_string = re.sub('\.\d+$', '', time_string).replace('T', ' ')
        dt = datetime.datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S')
    epoch = calendar.timegm(dt.timetuple()) + tz_diff
    if truncate:
        epoch = int(epoch / 86400) * 86400
    return str(epoch * 1000)


def get_value_presentation(field_name, field_type, value):
    if field_name.lower() == 'estimation':
        if field_type == 'period':
            value = int(int(value) / 60)
        elif field_type == 'integer':
            value = int(int(value) / 3600)
        return str(value)
    if field_type == 'date':
        return to_unix_date(value)
    if field_type == 'integer' or field_type == 'period':
        return str(value)
    if field_type == 'string':
        return value
    if 'name' in value:
        return value['name']
    if 'value' in value:
        return value['value']


@ignore_youtrack_exceptions
def process_attachments(source, target, issue, replace):
    def get_attachment_hash(attach):
        return attach.name + '\n' + attach.created

    if 'attachment' not in issue['fields']:
        return
    issue_id = issue['key']
    existing_attachments = dict()
    for a in target.getAttachments(issue_id):
        existing_attachments[get_attachment_hash(a)] = a
    for jira_attachment in issue['fields']['attachment']:
        attachment = JiraAttachment(jira_attachment, source)
        attachment_hash = get_attachment_hash(attachment)
        if attachment_hash in existing_attachments and not replace:
            continue
        if 'author' in jira_attachment:
            create_user(target, jira_attachment['author'])
        attachment_name = attachment.name
        if isinstance(attachment_name, unicode):
            attachment_name = attachment_name.encode('utf-8')
        try:
            print 'Creating attachment %s for issue %s' % \
                  (attachment_name, issue_id)
            target.createAttachmentFromAttachment(issue_id, attachment)
        except BaseException, e:
            print 'Cannot create attachment %s' % attachment_name
            print e
            continue
        if not replace:
            continue
        old_attachment = existing_attachments.get(attachment_hash)
        if not old_attachment:
            continue
        try:
            print 'Deleting old version of attachment %s for issue %s' % \
                  (attachment_name, issue_id)
            target.deleteAttachment(issue_id, old_attachment.id)
        except BaseException, e:
            print 'Cannot delete old version of attachment %s' % attachment_name
            print e


@ignore_youtrack_exceptions
def process_worklog(source, target, issue):
    worklog = source.get_worklog(issue['key'])
    if worklog:
        work_items = []
        for w in worklog['worklogs']:
            create_user(target, w['author'])
            work_item = WorkItem()
            work_item.authorLogin = w['author']['name']
            work_item.date = to_unix_date(w['started'], truncate=True)
            if 'comment' in w:
                work_item.description = w['comment']
            work_item.duration = int(int(w['timeSpentSeconds']) / 60)
            work_items.append(work_item)
            # target.createWorkItem(issue['key'], work_item)
        target.importWorkItems(issue['key'], work_items)


def jira2youtrack(source_url, source_login, source_password,
                  target_url, target_login, target_password,
                  projects, flags, batch_size, field_mappings,
                  value_mappings, description_fields, skip_fields):
    print 'source_url   : ' + source_url
    print 'source_login : ' + source_login
    print 'target_url   : ' + target_url
    print 'target_login : ' + target_login

    source = JiraClient(source_url, source_login, source_password)
    target = Target(target_url, target_login, target_password)

    issue_links = []

    for (project_id, start, end) in projects:
        try:
            target.createProjectDetailed(project_id, project_id, '', target_login)
        except YouTrackException:
            pass

        project = ProjectHelper(target, project_id, field_mappings,
                                value_mappings, description_fields, skip_fields)

        while True:
            _end = start + batch_size - 1
            if end and _end > end:
                _end = end
            if start > _end:
                break
            print 'Processing issues: %s [%d .. %d]' % (project_id, start, _end)
            try:
                jira_issues = source.get_issues(project_id, start, _end)
                start += batch_size
                if not (jira_issues or end):
                    break
                # Filter out moved issues
                jira_issues = [issue for issue in jira_issues
                               if issue['key'].startswith('%s-' % project_id)]
                if flags & FI_ISSUES:
                    issues2import = []
                    for issue in jira_issues:
                        issues2import.append(project.build_issue(issue))
                    if not issues2import:
                        continue
                    target.importIssues(project_id, '%s assignees' % project_id, issues2import)
            except YouTrackException, e:
                print e
                continue
            for issue in jira_issues:
                if flags & FI_LINKS:
                    process_links(target, issue, issue_links)
                if flags & FI_LABELS:
                    process_labels(target, issue)
                if flags & FI_ATTACHMENTS:
                    process_attachments(source, target, issue, flags & FI_REPLACE_ATTACHMENTS > 0)
                if flags & FI_WORK_LOG:
                    process_worklog(source, target, issue)

    if flags & FI_LINKS:
        for link in issue_links:
            target.importLinks([link])


class JiraAttachment(object):
    def __init__(self, attach, source):
        if 'author' in attach:
            self.authorLogin = attach['author']['name'].replace(' ', '_')
        else:
            self.authorLogin = 'root'
        self._url = attach['content']
        self.name = attach['filename']
        self.created = to_unix_date(attach['created'])
        self._source = source

    def getContent(self):
        return urllib2.urlopen(
            urllib2.Request(self._url, headers=self._source._headers))


class Target(Connection):
    def __init__(self, url, login=None, password=None, proxy_info=None, api_key=None):
        super(Target, self).__init__(url, login, password, proxy_info, api_key)
        self.users = []

    def createUser(self, user):
        login = user['login']
        if login in self.users:
            return
        super(Target, self).createUser(user)
        self.users.append(login)

    def createUserDetailed(self, login, fullName, email, jabber):
        self.createUser({
            'login': login,
            'fullName': fullName,
            'email': email,
            'jabber': jabber
        })


class ProjectHelper(object):
    def __init__(self, connection, id, fields_mapping, value_mappings, description_fields, skip_fields):
        super(ProjectHelper, self).__init__()
        self.id = id
        self.target = connection
        self.fields_mapping = fields_mapping
        self.value_mappings = value_mappings
        self.description_fields = description_fields
        self.skip_fields = skip_fields
        self.bundles = dict()
        self.known_fields = []

    def create_bundle(self, field_name, field_type, value):
        if field_name in self.bundles and value in self.bundles[field_name]:
            return
        project_field = self.target.getProjectCustomField(self.id, field_name)
        bundle = self.target.getBundle(field_type, project_field.bundle)
        self.target.addValueToBundle(bundle, value)
        if field_name not in self.bundles:
            self.bundles[field_name] = []
        self.bundles[field_name].append(value)

    def get_pcf(self):
        return [f.name.lower() for f in self.target.getProjectCustomFields(self.id)]

    def get_cf(self):
        return [f.name.lower() for f in self.target.getCustomFields()]

    def create_cf(self, field_name, type_name, is_private=False,
                  default_visibility=True, auto_attached=False,
                  additional_params=dict([])):
        return self.target.createCustomFieldDetailed(
            field_name, type_name, is_private, default_visibility,
            auto_attached, additional_params)

    def create_pcf(self, field_name, empty_field_text, params=None):
        return self.target.createProjectCustomFieldDetailed(self.id, field_name, empty_field_text, params)

    def describe_field(self, field):
        field_lc = field.lower()
        is_mapped = field_lc in self.fields_mapping
        if field_lc == 'description':
            return None, None
        if field_lc in self.description_fields and not is_mapped:
            return None, None
        if field_lc in self.skip_fields:
            return None, None
        if is_mapped:
            field_name, field_type = self.fields_mapping[field_lc]
        else:
            field_name = get_yt_field_name(field)
            field_type = get_yt_field_type(field_name)
        return field_name, field_type

    def build_description(self, issue):
        description = ""
        for field in self.description_fields:
            label = issue['names'].get(field, None)
            if not label:
                label = field
            value = issue['fields'].get(field, None)
            if value:
                if isinstance(value, dict):
                    value = value['value']
                if isinstance(value, basestring):
                    description += "'''%s''':\r\n" % label
                    description += value + "\r\n"
        return description + (issue['fields'].get('description', None) or '')

    def build_value(self, field_name, field_type, value):
        if isinstance(value, dict):
            value['name'] = get_yt_field_value(field_name, value['name'], self.value_mappings)
        else:
            value = get_yt_field_value(field_name, value, self.value_mappings)
        return self.create_value(field_name, field_type, value)

    def build_comment(self, comment):
        yt_comment = youtrack.Comment()
        yt_comment.text = comment['body']
        comment_author_name = "guest"
        if 'author' in comment:
            comment_author = comment['author']
            create_user(self.target, comment_author)
            comment_author_name = comment_author['name']
        yt_comment.author = comment_author_name.replace(' ', '_')
        yt_comment.created = to_unix_date(comment['created'])
        yt_comment.updated = to_unix_date(comment['updated'])
        return yt_comment

    def build_issue(self, issue):
        self.create_fields(issue['fields'].keys())
        yt_issue = youtrack.Issue()
        yt_issue.numberInProject = issue['key'][(issue['key'].find('-') + 1):]
        yt_issue['comments'] = []
        yt_issue['description'] = self.build_description(issue)
        for field, value in issue['fields'].items():
            if value is None:
                continue
            field_name, field_type = self.describe_field(field)
            if field_name is None:
                continue
            if field_name == 'comment':
                for comment in value['comments']:
                    yt_issue['comments'].append(self.build_comment(comment))
            if field_type is None:
                if _debug:
                    print 'DEBUG: unclassified field', field
                continue

            is_list = isinstance(value, list)
            values = []
            for v in (value if is_list else [value]):
                values.append(self.build_value(field_name, field_type, v))
            if len(values):
                if is_list:
                    yt_issue[field_name] = values
                else:
                    yt_issue[field_name] = values[0]
        if 'reporterName' not in yt_issue:
            yt_issue['reporterName'] = 'root'
        return yt_issue

    def create_fields(self, fields):
        project_fields = None
        custom_fields = None
        for field in fields:
            # optimization
            if field.lower() in self.known_fields:
                continue
            self.known_fields.append(field.lower())

            field_name, field_type = self.describe_field(field)
            if field_type is None or field_name is None:
                continue

            # case sensitive
            if field_name in jira.EXISTING_FIELDS:
                continue

            if project_fields is None:
                project_fields = self.get_pcf()
            if custom_fields is None:
                custom_fields = self.get_cf()

            if field_name.lower() in project_fields:
                continue

            if field_name.lower() not in custom_fields:
                self.create_cf(field_name, field_type)

            if field_type in ['string', 'date', 'integer', 'period']:
                try:
                    self.create_pcf(field_name, "No " + field_name)
                except YouTrackException, e:
                    if e.response.status == 409:
                        print e
                    else:
                        raise e
            else:
                bundle_name = "%s: %s" % (self.id, field_name)
                create_bundle_safe(self.target, bundle_name, field_type)
                try:
                    self.create_pcf(field_name, "No " + field_name, {'bundle': bundle_name})
                except YouTrackException, e:
                    if e.response.status == 409:
                        print e
                    else:
                        raise e

    def create_value(self, field_name, field_type, value):
        if field_type.startswith('user'):
            create_user(self.target, value)
            value['name'] = value['name'].replace(' ', '_')
        value = get_value_presentation(field_name, field_type, value)
        if field_name in jira.EXISTING_FIELDS:
            return value
        if field_type in ['string', 'date', 'integer', 'period']:
            return value
        value = re.sub(r'[<>/]', '_', value)
        try:
            self.create_bundle(field_name, field_type, value)
        except YouTrackException:
            pass
        return value


if __name__ == '__main__':
    main()
