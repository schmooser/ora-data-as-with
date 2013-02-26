#!/usr/bin/python

import datetime
import cx_Oracle


class OraDataAsWith(object):
    NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
    ALTER_SESSION = "alter session set nls_date_format='%s'" % NLS_DATE_FORMAT

    def __init__(self, connection_string, file_query='query.sql',
                 file_yield='yield.sql', file_result='result.sql'):

        self.file_query = file_query
        self.file_yield = file_yield
        self.file_result = file_result
        self.connection_string = connection_string

    def load_data(self):

        db = cx_Oracle.connect(self.connection_string)
        cursor = db.cursor()

        cursor.execute(self.ALTER_SESSION)
        cursor.execute(open(self.file_query, 'r').read())

        titles = [x[0] for x in cursor.description]

        def item_to_str(x):
            ii = isinstance
            if x is None:
                return 'NULL'
            if ii(x, int) or ii(x, float) or ii(x, long):
                return x
            if ii(x, datetime.datetime):
                return "to_date('%s','%s')" % (x, self.NLS_DATE_FORMAT)
            return "'%s'" % x

        wrap = lambda x: '%s %s' % (item_to_str(x[0]), x[1])

        out = ["select %s from dual" % ', '.join(map(wrap, zip(row, titles)))
               for row in cursor]

        with_stmt = ' union all\n'.join(out)
        return with_stmt

    def process(self):
        with_stmt = self.load_data()
        sql_stmt = open(self.file_yield, 'r').readlines()
        output = open(self.file_result, 'w')

        output.write('set define off;\n%s;\n' % self.ALTER_SESSION)

        for line in sql_stmt:
            if '/*WITH*/' in line:
                line = with_stmt
            output.write(line)

        output.write('\nexit;\n')
        output.close()
        print 'File %s processed' % self.file_query

if __name__ == '__main__':
    x = OraDataAsWith(connection_string='dmsfr/dmsfr@dwstdev1')
    x.process()

