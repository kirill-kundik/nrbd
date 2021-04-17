import psycopg2
import psycopg2.extras


class Database:
    def __init__(self, conn, debug=False):
        self._conn = conn
        self._debug = debug

    def commit(self):
        self._conn.commit()

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

    def polim(self, base_name, region):
        sql = f"WITH variables_count AS (SELECT position, COUNT(DISTINCT(value)) AS val_num " \
              f"FROM ((public.sequence INNER JOIN public.fasta_position ON public.sequence.id = public.fasta_position.sequence_id) " \
              f"INNER JOIN public.person ON public.person.sequence_id = public.sequence.id)  " \
              f"INNER JOIN public.region ON public.region.id = public.person.region_id " \
              f"WHERE sequence_type=0 OR public.sequence.name=%s "
        params = [base_name]

        if region != 'ALL':
            params.append(region)
            sql += "AND public.region.name = %s "

        sql += f"GROUP BY position) " \
               f"SELECT COUNT(*) " \
               f"FROM variables_count " \
               f"WHERE val_num > 1;"

        return self.execute_query(sql, params, dict_return=True)

    def calculate_wild(self, region):
        # TODO + rework with region
        try:
            sql = f"WITH letter_on_position_count AS (SELECT position, value, COUNT(value) AS letter_count " \
                  f"FROM ((public.sequence INNER JOIN public.fasta_position ON public.sequence.id = public.fasta_position.sequence_id) " \
                  f"INNER JOIN public.person ON public.person.sequence_id = public.sequence.id)  " \
                  f"INNER JOIN public.region ON public.region.id = public.person.region_id " \
                  f"WHERE sequence_type = 0 "

            params = [region]
            if region != 'ALL':
                sql += "AND public.region.name = %s "

            sql += f"GROUP BY position, value), " \
                  f"max_count AS (SELECT position, MAX(letter_count) AS max_count " \
                  f"FROM letter_on_position_count " \
                  f"GROUP BY position " \
                  f"ORDER BY position), " \
                  f"most_popular AS (SELECT letter_on_position_count.position, value " \
                  f"FROM letter_on_position_count INNER JOIN max_count ON letter_on_position_count.position = max_count.position " \
                  f"WHERE letter_count = max_count " \
                  f"ORDER BY position) " \
                  f"INSERT INTO public.sequence (sequence_type, name, fasta) VALUES (2, 'WILD_TYPE_{region}', (SELECT STRING_AGG ( " \
                  f"most_popular.value, '' " \
                  f"ORDER BY most_popular.position ) most_popular " \
                  f"FROM most_popular LIMIT 1) );"

            self.execute_query(sql, params, fetch=False, dict_return=True)
        except psycopg2.errors.UniqueViolation:
            pass

    def distribution(self, base_name, region):
        sql = """
WITH base_positions AS (SELECT position AS base_pos, value AS eva_value
                        FROM public.sequence
                                 INNER JOIN public.fasta_position
                                            ON public.sequence.id = public.fasta_position.sequence_id
                        WHERE name = %s),
     fasta_diff AS (SELECT public.person.id, COUNT(*) AS diff_num
                    FROM (((public.sequence INNER JOIN public.person ON public.person.sequence_id = public.sequence.id) INNER JOIN public.region ON public.person.region_id = public.region.id)
                        INNER JOIN public.fasta_position ON public.sequence.id = public.fasta_position.sequence_id)
                             INNER JOIN base_positions ON base_pos = public.fasta_position.position
                    WHERE value != eva_value
                      AND sequence_type = 0
        """
        params = [base_name]

        if region != 'ALL':
            params.append(region)
            sql += "AND public.region.name = %s "

        sql += """
GROUP BY public.person.id),
     rosp AS (SELECT diff_num, COUNT(*) AS frequency
              FROM fasta_diff
              GROUP BY diff_num
              ORDER BY diff_num),
     frequency_summ AS (SELECT SUM(frequency) AS f_s
                        FROM rosp)
SELECT diff_num, frequency, (frequency / (SELECT f_s FROM frequency_summ)) AS p
FROM rosp
        """

        return self.execute_query(sql, params, dict_return=True)

    def math_expectation(self, base_name, region):
        sql = """
WITH base_positions AS (SELECT position AS base_pos, value AS eva_value
                        FROM public.sequence
                                 INNER JOIN public.fasta_position
                                            ON public.sequence.id = public.fasta_position.sequence_id
                        WHERE name = %s),
     fasta_diff AS (SELECT public.person.id, COUNT(*) AS diff_num
                    FROM (((public.sequence INNER JOIN public.person ON public.person.sequence_id = public.sequence.id) INNER JOIN public.region ON public.person.region_id = public.region.id)
                        INNER JOIN public.fasta_position ON public.sequence.id = public.fasta_position.sequence_id)
                             INNER JOIN base_positions ON base_pos = public.fasta_position.position
                    WHERE value != eva_value
                      AND sequence_type = 0
        """
        params = [base_name]

        if region != 'ALL':
            params.append(region)
            sql += "AND public.region.name = %s "

        sql += """
GROUP BY public.person.id),
     rosp AS (SELECT diff_num, COUNT(*) AS frequency
              FROM fasta_diff
              GROUP BY diff_num
              ORDER BY diff_num),
     frequency_summ AS (SELECT SUM(frequency) AS f_s
                        FROM rosp),
     probability_rosp AS (SELECT diff_num, frequency, (frequency / (SELECT f_s FROM frequency_summ)) AS p
                          FROM rosp)
SELECT SUM(diff_num * p) AS math_expectation
FROM probability_rosp
        """

        return self.execute_query(sql, params, dict_return=True)

    def std(self, base_name, region):
        sql = """
WITH base_positions AS (SELECT position AS base_pos, value AS eva_value
                        FROM public.sequence
                                 INNER JOIN public.fasta_position
                                            ON public.sequence.id = public.fasta_position.sequence_id
                        WHERE name = %s),
     fasta_diff AS (SELECT public.person.id, COUNT(*) AS diff_num
                    FROM (((public.sequence INNER JOIN public.person ON public.person.sequence_id = public.sequence.id) INNER JOIN public.region ON public.person.region_id = public.region.id)
                        INNER JOIN public.fasta_position ON public.sequence.id = public.fasta_position.sequence_id)
                             INNER JOIN base_positions ON base_pos = public.fasta_position.position
                    WHERE value != eva_value
                      AND sequence_type = 0
        """
        params = [base_name]

        if region != 'ALL':
            params.append(region)
            sql += 'AND public.region.name = %s '

        sql += """
GROUP BY public.person.id),
     rosp AS (SELECT diff_num, COUNT(*) AS frequency
              FROM fasta_diff
              GROUP BY diff_num
              ORDER BY diff_num),
     frequency_summ AS (SELECT SUM(frequency) AS f_s
                        FROM rosp),
     probability_rosp AS (SELECT diff_num, frequency, (frequency / (SELECT f_s FROM frequency_summ)) AS p
                          FROM rosp),
     math_expectation AS (SELECT SUM(diff_num * p) AS math_expct
                          FROM probability_rosp)
SELECT |/SUM((diff_num - (SELECT math_expct FROM math_expectation)) *
             (diff_num - (SELECT math_expct FROM math_expectation)) * p) AS standart_dev
FROM probability_rosp
        """

        return self.execute_query(sql, params, dict_return=True)

    def mode(self, base_name, region):
        sql = """
WITH base_positions AS (SELECT position AS base_pos, value AS eva_value
                        FROM public.sequence
                                 INNER JOIN public.fasta_position
                                            ON public.sequence.id = public.fasta_position.sequence_id
                        WHERE name = %s),
     fasta_diff AS (SELECT public.person.id, COUNT(*) AS diff_num
                    FROM (((public.sequence INNER JOIN public.person ON public.person.sequence_id = public.sequence.id) INNER JOIN public.region ON public.person.region_id = public.region.id)
                        INNER JOIN public.fasta_position ON public.sequence.id = public.fasta_position.sequence_id)
                             INNER JOIN base_positions ON base_pos = public.fasta_position.position
                    WHERE value != eva_value
                      AND sequence_type = 0
        """
        params = [base_name]

        if region != 'ALL':
            params.append(region)
            sql += 'AND public.region.name = %s '

        sql += """
GROUP BY public.person.id),
     rosp AS (SELECT diff_num, COUNT(*) AS frequency
              FROM fasta_diff
              GROUP BY diff_num
              ORDER BY diff_num),
     frequency_summ AS (SELECT SUM(frequency) AS f_s
                        FROM rosp),
     probability_rosp AS (SELECT diff_num, frequency, (frequency / (SELECT f_s FROM frequency_summ)) AS p
                          FROM rosp)
SELECT diff_num
FROM probability_rosp
WHERE frequency = (SELECT MAX(frequency)
                   FROM probability_rosp)
        """

        return self.execute_query(sql, params, dict_return=True)

    def min_value(self, base_name, region):
        sql = """
WITH base_positions AS (SELECT position AS base_pos, value AS eva_value
                        FROM public.sequence
                                 INNER JOIN public.fasta_position
                                            ON public.sequence.id = public.fasta_position.sequence_id
                        WHERE name = %s),
     fasta_diff AS (SELECT public.person.id, COUNT(*) AS diff_num
                    FROM (((public.sequence INNER JOIN public.person ON public.person.sequence_id = public.sequence.id) INNER JOIN public.region ON public.person.region_id = public.region.id)
                        INNER JOIN public.fasta_position ON public.sequence.id = public.fasta_position.sequence_id)
                             INNER JOIN base_positions ON base_pos = public.fasta_position.position
                    WHERE value != eva_value
                      AND sequence_type = 0
        """
        params = [base_name]

        if region != 'ALL':
            params.append(region)
            sql += 'AND public.region.name = %s '

        sql += """
GROUP BY public.person.id),
     rosp AS (SELECT diff_num, COUNT(*) AS frequency
              FROM fasta_diff
              GROUP BY diff_num
              ORDER BY diff_num),
     frequency_summ AS (SELECT SUM(frequency) AS f_s
                        FROM rosp),
     probability_rosp AS (SELECT diff_num, frequency, (frequency / (SELECT f_s FROM frequency_summ)) AS p
                          FROM rosp)
SELECT MIN(diff_num)
FROM probability_rosp
        """

        return self.execute_query(sql, params, dict_return=True)

    def max_value(self, base_name, region):
        sql = """
WITH base_positions AS (SELECT position AS base_pos, value AS eva_value
                        FROM public.sequence
                                 INNER JOIN public.fasta_position
                                            ON public.sequence.id = public.fasta_position.sequence_id
                        WHERE name = %s),
     fasta_diff AS (SELECT public.person.id, COUNT(*) AS diff_num
                    FROM (((public.sequence INNER JOIN public.person ON public.person.sequence_id = public.sequence.id) INNER JOIN public.region ON public.person.region_id = public.region.id)
                        INNER JOIN public.fasta_position ON public.sequence.id = public.fasta_position.sequence_id)
                             INNER JOIN base_positions ON base_pos = public.fasta_position.position
                    WHERE value != eva_value
                      AND sequence_type = 0
        """
        params = [base_name]

        if region != 'ALL':
            params.append(region)
            sql += 'AND public.region.name = %s '

        sql += """
GROUP BY public.person.id),
     rosp AS (SELECT diff_num, COUNT(*) AS frequency
              FROM fasta_diff
              GROUP BY diff_num
              ORDER BY diff_num),
     frequency_summ AS (SELECT SUM(frequency) AS f_s
                        FROM rosp),
     probability_rosp AS (SELECT diff_num, frequency, (frequency / (SELECT f_s FROM frequency_summ)) AS p
                          FROM rosp)
SELECT MAX(diff_num)
FROM probability_rosp
        """

        return self.execute_query(sql, params, dict_return=True)

    def coeff(self, base_name, region):
        sql = """
WITH base_positions AS (SELECT position AS base_pos, value AS eva_value
                        FROM public.sequence
                                 INNER JOIN public.fasta_position
                                            ON public.sequence.id = public.fasta_position.sequence_id
                        WHERE name = %s),
     fasta_diff AS (SELECT public.person.id, COUNT(*) AS diff_num
                    FROM (((public.sequence INNER JOIN public.person ON public.person.sequence_id = public.sequence.id) INNER JOIN public.region ON public.person.region_id = public.region.id)
                        INNER JOIN public.fasta_position ON public.sequence.id = public.fasta_position.sequence_id)
                             INNER JOIN base_positions ON base_pos = public.fasta_position.position
                    WHERE value != eva_value
                      AND sequence_type = 0
        """
        params = [base_name]

        if region != 'ALL':
            params.append(region)
            sql += 'AND public.region.name = %s '

        sql += """
GROUP BY public.person.id),
     rosp AS (SELECT diff_num, COUNT(*) AS frequency
              FROM fasta_diff
              GROUP BY diff_num
              ORDER BY diff_num),
     frequency_summ AS (SELECT SUM(frequency) AS f_s
                        FROM rosp),
     probability_rosp AS (SELECT diff_num, frequency, (frequency / (SELECT f_s FROM frequency_summ)) AS p
                          FROM rosp),
     math_expectation AS (SELECT SUM(diff_num * p) AS math_expct
                          FROM probability_rosp),
     standart_deviation AS (SELECT |/SUM((diff_num - (SELECT math_expct FROM math_expectation)) *
                                         (diff_num - (SELECT math_expct FROM math_expectation)) * p) AS standart_dev
                            FROM probability_rosp)
SELECT (SELECT standart_dev FROM standart_deviation) / (SELECT math_expct FROM math_expectation) as koef
        """

        return self.execute_query(sql, params, dict_return=True)

    def rosp_each_to_each(self, region):
        # TODO: rework with region
        sql = f"WITH sequence_with_duplicate AS ( " \
              f"SELECT public.person.id, sequence_id, fasta, sequence_type " \
              f"FROM public.sequence INNER JOIN public.person ON public.person.sequence_id = public.sequence.id " \
              f"WHERE sequence_type = 0 " \
              f"), " \
              f"help_table AS (SELECT sequence_1.id AS id_1, sequence_2.id AS id_2,  " \
              f"sequence_1.sequence_id AS sequence_id_1, sequence_2.sequence_id AS sequence_id_2,  " \
              f"sequence_1.fasta AS fasta_1, sequence_2.fasta AS fasta_2 " \
              f"FROM sequence_with_duplicate AS sequence_1, sequence_with_duplicate AS sequence_2 " \
              f"WHERE sequence_1.id != sequence_2.id), " \
              f"fasta_diff AS (SELECT COUNT(*) AS diff_num " \
              f"FROM (help_table INNER JOIN public.fasta_position AS fasta_position_1 ON help_table.sequence_id_1 = fasta_position_1.sequence_id)  " \
              f"INNER JOIN public.fasta_position AS fasta_position_2 ON help_table.sequence_id_2 = fasta_position_2.sequence_id " \
              f"WHERE fasta_position_1.position = fasta_position_2.position AND fasta_position_1.value != fasta_position_2.value " \
              f"GROUP BY help_table.id_1, help_table.id_2), " \
              f"rosp AS (SELECT diff_num, COUNT(*) AS frequency " \
              f"FROM fasta_diff " \
              f"GROUP BY diff_num " \
              f"ORDER BY diff_num), " \
              f"frequency_summ AS (SELECT SUM(frequency) AS f_s " \
              f"FROM rosp) " \
              f"SELECT diff_num, frequency, (frequency/(SELECT f_s FROM frequency_summ)) AS p " \
              f"FROM rosp;"

        res = self.execute_query(sql, None, dict_return=True)
        return res

    def math_expectation_each_to_each(self, region):
        # TODO: rework with region
        sql = f"WITH sequence_with_duplicate AS ( " \
              f"SELECT public.person.id, fasta, sequence_type " \
              f"FROM public.sequence INNER JOIN public.person ON public.person.sequence_id = public.sequence.id " \
              f"WHERE sequence_type = 0), " \
              f"help_table AS (SELECT sequence_1.id AS id_1, sequence_2.id AS id_2, sequence_1.fasta AS fasta_1, sequence_2.fasta AS fasta_2 " \
              f"FROM sequence_with_duplicate AS sequence_1, sequence_with_duplicate AS sequence_2 " \
              f"WHERE sequence_1.id != sequence_2.id), " \
              f"fasta_diff AS (SELECT COUNT(*) AS diff_num " \
              f"FROM (help_table INNER JOIN public.fasta_position AS fasta_position_1 ON help_table.id_1 = fasta_position_1.sequence_id) " \
              f"INNER JOIN public.fasta_position AS fasta_position_2 ON help_table.id_2 = fasta_position_2.sequence_id " \
              f"WHERE fasta_position_1.position = fasta_position_2.position AND fasta_position_1.value != fasta_position_2.value " \
              f"GROUP BY help_table.id_1, help_table.id_2, help_table.fasta_1, help_table.fasta_2 " \
              f"ORDER BY help_table.id_1, help_table.id_2), " \
              f"rosp AS (SELECT diff_num, COUNT(*) AS frequency " \
              f"FROM fasta_diff " \
              f"GROUP BY diff_num " \
              f"ORDER BY diff_num), " \
              f"frequency_summ AS (SELECT SUM(frequency) AS f_s " \
              f"FROM rosp), " \
              f"probability_rosp AS (SELECT diff_num, (frequency/(SELECT f_s FROM frequency_summ)) AS p " \
              f"FROM rosp) " \
              f"SELECT SUM(diff_num*p) AS math_expectation " \
              f"FROM probability_rosp;"

        res = self.execute_query(sql, None, dict_return=True)
        return res

    def std_each_to_each(self, region):
        # TODO: rework with region
        sql = f"WITH sequence_with_duplicate AS ( " \
              f"SELECT public.person.id, fasta, sequence_type " \
              f"FROM public.sequence INNER JOIN public.person ON public.person.sequence_id = public.sequence.id " \
              f"WHERE sequence_type = 0 " \
              f"),  " \
              f"help_table AS (SELECT sequence_1.id AS id_1, sequence_2.id AS id_2, sequence_1.fasta AS fasta_1, sequence_2.fasta AS fasta_2 " \
              f"FROM sequence_with_duplicate AS sequence_1, sequence_with_duplicate AS sequence_2 " \
              f"WHERE sequence_1.id != sequence_2.id), " \
              f"fasta_diff AS (SELECT COUNT(*) AS diff_num " \
              f"FROM (help_table INNER JOIN public.fasta_position AS fasta_position_1 ON help_table.id_1 = fasta_position_1.sequence_id)  " \
              f"INNER JOIN public.fasta_position AS fasta_position_2 ON help_table.id_2 = fasta_position_2.sequence_id " \
              f"WHERE fasta_position_1.position = fasta_position_2.position AND fasta_position_1.value != fasta_position_2.value " \
              f"GROUP BY help_table.id_1, help_table.id_2, help_table.fasta_1, help_table.fasta_2 " \
              f"ORDER BY help_table.id_1, help_table.id_2), " \
              f"rosp AS (SELECT diff_num, COUNT(*) AS frequency " \
              f"FROM fasta_diff " \
              f"GROUP BY diff_num " \
              f"ORDER BY diff_num), " \
              f"frequency_summ AS (SELECT SUM(frequency) AS f_s " \
              f"FROM rosp), " \
              f"probability_rosp AS (SELECT diff_num, frequency, (frequency/(SELECT f_s FROM frequency_summ)) AS p " \
              f"FROM rosp), " \
              f"math_expectation AS (SELECT SUM(diff_num*p) AS math_expct " \
              f"FROM probability_rosp) " \
              f"SELECT |/SUM((diff_num - (SELECT math_expct FROM math_expectation)) * (diff_num - (SELECT math_expct FROM math_expectation))*p) AS standart_dev " \
              f"FROM probability_rosp;"

        res = self.execute_query(sql, None, dict_return=True)
        return res

    def mode_each_to_each(self, region):
        # TODO: rework with region
        sql = f"WITH sequence_with_duplicate AS ( " \
              f"SELECT public.person.id, fasta, sequence_type " \
              f"FROM public.sequence INNER JOIN public.person ON public.person.sequence_id = public.sequence.id " \
              f"WHERE sequence_type = 0),  " \
              f"help_table AS (SELECT sequence_1.id AS id_1, sequence_2.id AS id_2, sequence_1.fasta AS fasta_1, sequence_2.fasta AS fasta_2 " \
              f"FROM sequence_with_duplicate AS sequence_1, sequence_with_duplicate AS sequence_2 " \
              f"WHERE sequence_1.id != sequence_2.id), " \
              f"fasta_diff AS (SELECT COUNT(*) AS diff_num " \
              f"FROM (help_table INNER JOIN public.fasta_position AS fasta_position_1 ON help_table.id_1 = fasta_position_1.sequence_id)  " \
              f"INNER JOIN public.fasta_position AS fasta_position_2 ON help_table.id_2 = fasta_position_2.sequence_id " \
              f"WHERE fasta_position_1.position = fasta_position_2.position AND fasta_position_1.value != fasta_position_2.value " \
              f"GROUP BY help_table.id_1, help_table.id_2, help_table.fasta_1, help_table.fasta_2 " \
              f"ORDER BY help_table.id_1, help_table.id_2), " \
              f"rosp AS (SELECT diff_num, COUNT(*) AS frequency " \
              f"FROM fasta_diff " \
              f"GROUP BY diff_num " \
              f"ORDER BY diff_num), " \
              f"frequency_summ AS (SELECT SUM(frequency) AS f_s " \
              f"FROM rosp), " \
              f"probability_rosp AS (SELECT diff_num, frequency, (frequency/(SELECT f_s FROM frequency_summ)) AS p " \
              f"FROM rosp) " \
              f"SELECT diff_num, frequency , p " \
              f"FROM probability_rosp  " \
              f"WHERE p = (SELECT MAX(p) " \
              f"FROM probability_rosp);"

        res = self.execute_query(sql, None, dict_return=True)
        return res

    def min_value_each_to_each(self, region):
        # TODO: rework with region
        sql = f"WITH sequence_with_duplicate AS ( " \
              f"SELECT public.person.id, fasta, sequence_type " \
              f"FROM public.sequence INNER JOIN public.person ON public.person.sequence_id = public.sequence.id " \
              f"WHERE sequence_type = 0),  " \
              f"help_table AS (SELECT sequence_1.id AS id_1, sequence_2.id AS id_2, sequence_1.fasta AS fasta_1, sequence_2.fasta AS fasta_2 " \
              f"FROM sequence_with_duplicate AS sequence_1, sequence_with_duplicate AS sequence_2 " \
              f"WHERE sequence_1.id != sequence_2.id), " \
              f"fasta_diff AS (SELECT COUNT(*) AS diff_num " \
              f"FROM (help_table INNER JOIN public.fasta_position AS fasta_position_1 ON help_table.id_1 = fasta_position_1.sequence_id)  " \
              f"INNER JOIN public.fasta_position AS fasta_position_2 ON help_table.id_2 = fasta_position_2.sequence_id " \
              f"WHERE fasta_position_1.position = fasta_position_2.position AND fasta_position_1.value != fasta_position_2.value " \
              f"GROUP BY help_table.id_1, help_table.id_2, help_table.fasta_1, help_table.fasta_2 " \
              f"ORDER BY help_table.id_1, help_table.id_2), " \
              f"rosp AS (SELECT diff_num, COUNT(*) AS frequency " \
              f"FROM fasta_diff " \
              f"GROUP BY diff_num " \
              f"ORDER BY diff_num), " \
              f"frequency_summ AS (SELECT SUM(frequency) AS f_s " \
              f"FROM rosp), " \
              f"probability_rosp AS (SELECT diff_num, frequency, (frequency/(SELECT f_s FROM frequency_summ)) AS p " \
              f"FROM rosp) " \
              f"SELECT MIN(diff_num) " \
              f"FROM probability_rosp;"

        res = self.execute_query(sql, None, dict_return=True)
        return res

    def max_value_each_to_each(self, region):
        # TODO: rework with region
        sql = f"WITH sequence_with_duplicate AS ( " \
              f"SELECT public.person.id, fasta, sequence_type " \
              f"FROM public.sequence INNER JOIN public.person ON public.person.sequence_id = public.sequence.id " \
              f"WHERE sequence_type = 0),  " \
              f"help_table AS (SELECT sequence_1.id AS id_1, sequence_2.id AS id_2, sequence_1.fasta AS fasta_1, sequence_2.fasta AS fasta_2 " \
              f"FROM sequence_with_duplicate AS sequence_1, sequence_with_duplicate AS sequence_2 " \
              f"WHERE sequence_1.id != sequence_2.id), " \
              f"fasta_diff AS (SELECT COUNT(*) AS diff_num " \
              f"FROM (help_table INNER JOIN public.fasta_position AS fasta_position_1 ON help_table.id_1 = fasta_position_1.sequence_id)  " \
              f"INNER JOIN public.fasta_position AS fasta_position_2 ON help_table.id_2 = fasta_position_2.sequence_id " \
              f"WHERE fasta_position_1.position = fasta_position_2.position AND fasta_position_1.value != fasta_position_2.value " \
              f"GROUP BY help_table.id_1, help_table.id_2, help_table.fasta_1, help_table.fasta_2 " \
              f"ORDER BY help_table.id_1, help_table.id_2), " \
              f"rosp AS (SELECT diff_num, COUNT(*) AS frequency " \
              f"FROM fasta_diff " \
              f"GROUP BY diff_num " \
              f"ORDER BY diff_num), " \
              f"frequency_summ AS (SELECT SUM(frequency) AS f_s " \
              f"FROM rosp), " \
              f"probability_rosp AS (SELECT diff_num, frequency, (frequency/(SELECT f_s FROM frequency_summ)) AS p " \
              f"FROM rosp) " \
              f"SELECT MAX(diff_num) " \
              f"FROM probability_rosp;"

        res = self.execute_query(sql, None, dict_return=True)
        return res

    def coeff_each_to_each(self, region):
        # TODO: rework with region
        sql = f"WITH sequence_with_duplicate AS ( " \
              f"SELECT public.person.id, fasta, sequence_type " \
              f"FROM public.sequence INNER JOIN public.person ON public.person.sequence_id = public.sequence.id " \
              f"WHERE sequence_type = 0 " \
              f"),  " \
              f"help_table AS (SELECT sequence_1.id AS id_1, sequence_2.id AS id_2, sequence_1.fasta AS fasta_1, sequence_2.fasta AS fasta_2 " \
              f"FROM sequence_with_duplicate AS sequence_1, sequence_with_duplicate AS sequence_2 " \
              f"WHERE sequence_1.id != sequence_2.id), " \
              f"fasta_diff AS (SELECT COUNT(*) AS diff_num " \
              f"FROM (help_table INNER JOIN public.fasta_position AS fasta_position_1 ON help_table.id_1 = fasta_position_1.sequence_id)  " \
              f"INNER JOIN public.fasta_position AS fasta_position_2 ON help_table.id_2 = fasta_position_2.sequence_id " \
              f"WHERE fasta_position_1.position = fasta_position_2.position AND fasta_position_1.value != fasta_position_2.value " \
              f"GROUP BY help_table.id_1, help_table.id_2, help_table.fasta_1, help_table.fasta_2 " \
              f"ORDER BY help_table.id_1, help_table.id_2), " \
              f"rosp AS (SELECT diff_num, COUNT(*) AS frequency " \
              f"FROM fasta_diff " \
              f"GROUP BY diff_num " \
              f"ORDER BY diff_num), " \
              f"frequency_summ AS (SELECT SUM(frequency) AS f_s " \
              f"FROM rosp), " \
              f"probability_rosp AS (SELECT diff_num, frequency, (frequency/(SELECT f_s FROM frequency_summ)) AS p " \
              f"FROM rosp), " \
              f"math_expectation AS (SELECT SUM(diff_num*p) AS math_expct " \
              f"FROM probability_rosp), " \
              f"standart_deviation AS (SELECT |/SUM((diff_num - (SELECT math_expct FROM math_expectation)) * (diff_num - (SELECT math_expct FROM math_expectation))*p) AS standart_dev " \
              f"FROM probability_rosp) " \
              f"SELECT (SELECT standart_dev FROM standart_deviation)/(SELECT math_expct FROM math_expectation) as koef;"

        res = self.execute_query(sql, None, dict_return=True)
        return res

    def get_distinct_regions(self):
        return [x[0] for x in self.execute_query('SELECT distinct name FROM region', None)]


if __name__ == '__main__':
    conn_ = psycopg2.connect("dbname='nrbd' user='postgres' host='localhost' password='gulayeva'")
    for r in Database(conn_).execute_query('SELECT 1+2 AS hello', None, dict_return=True):
        print(r['hello'])
