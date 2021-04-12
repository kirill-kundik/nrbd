import psycopg2
import psycopg2.extras


class Database:
    def __init__(self, conn, debug=False):
        self._conn = conn
        self._debug = debug

    def get_cursor(self, dict_return=False):
        return self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor) if dict_return else self._conn.cursor()

    def execute_query(self, query, params, fetch=True, dict_return=False, many=False):
        cursor = self.get_cursor(dict_return)

        if self._debug:
            print(f'DEBUG --- QUERY: {query}')
            print(f'DEBUG --- PARAMS: {params}')
            print(f'DEBUG --- FETCH: {fetch}, DICT RETURN: {dict_return}, EXECUTE MANY: {many}')

        if many:
            cursor.executemany(query, params)
        else:
            cursor.execute(query, params)

        return cursor.fetchall() if fetch else None

    def insert(self, table, fields, values, id_=True, many=False):
        values_len = len(values[0]) if many else len(values)
        sql = f'INSERT INTO {table} ({", ".join(fields) if len(fields) > 1 else fields[0]}) ' \
              f'VALUES ({"%s, ".join(["" for _ in range(values_len)])}%s)'

        if id_:
            sql += ' RETURNING id'

        return self.execute_query(sql, values, many=False)[0][0] if id_ else self.execute_query(sql, values, many=many,
                                                                                                fetch=False)

    def select(self, table, filter_=None, params=None, first_=True):
        sql = f'SELECT * FROM {table} WHERE TRUE'
        if filter_:
            for f in filter_:
                sql += f' AND {f}'
        if first_:
            sql += ' LIMIT 1'

        res = self.execute_query(sql, params, dict_return=True)

        if len(res) > 0:
            return res[0] if first_ else res
        else:
            return None

    def get_region(self, name):
        table_name = 'region'
        region = self.select(table_name, ['name = %s'], (name,))
        if not region:
            region = self.select(table_name, ['id = %s'], [self.insert(table_name, ['name'], [name])])

        return region

    def get_sequence(self, fasta_code, type_=None):
        conditions = ['fasta = %s']
        values = [fasta_code]

        if type_ is not None:
            conditions.append('sequence_type = %s')
            values.append(type_)

        return self.select('sequence', conditions, values)

    def get_person(self, url):
        return self.select('person', ['url = %s'], [url])

    def get_base_sequence(self, id_=None):
        filter_ = ['id = %s'] if id_ else None
        params = [id_] if id_ else None
        return self.select('base_sequence', filter_, params, first_=True)

    def insert_sequence_differences(self, params):
        self.insert('sequence_difference', ['position', 'value', 'sequence_id'], params, id_=False, many=True)

    def insert_fasta_positions(self, position, value, sequence_id):
        table_name = 'fasta_position'
        fields = ['position', 'value', 'sequence_id']
        values = [position, value, sequence_id]
        self.insert(table_name, fields, values)

    def insert_sequence(self, fasta, type_=None, name=None):
        # base_sequence = self.get_base_sequence(id_=base_sequence)
        # if not base_sequence:
        #     return

        table_name = 'sequence'
        fields = ['fasta']
        values = [fasta]

        if type_ is not None:
            fields.append('sequence_type')
            values.append(type_)
        # if base_sequence_id is not None:
        #     fields.append('base_sequence_id')
        #     values.append(base_sequence_id)

        if name is not None:
            fields.append('name')
            values.append(name)

        res = self.insert('sequence', fields, values)

        return self.select(table_name, ['id = %s'], [res])

    def insert_person(self, region_id, sequence_id, url):
        self.insert('person', ['region_id', 'sequence_id', 'url'], [region_id, sequence_id, url])

    def calculate_wild(self):
        pass


if __name__ == '__main__':
    conn_ = psycopg2.connect("dbname='nrbd' user='postgres' host='localhost' password='gulayeva'")
    for r in Database(conn_).execute_query('SELECT 1+2 AS hello', None, dict_return=True):
        print(r['hello'])
