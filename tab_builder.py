import psycopg2

import database
import xlsx_wrapper


class TabBuilder:
    def __init__(self, wrapper: xlsx_wrapper.XlsxWrapper, db: database.Database, dist_range: int = 20):
        self._dist_range: int = dist_range
        self._wrapper: xlsx_wrapper.XlsxWrapper = wrapper
        self._db: database.Database = db

    def build_distribution(self, tab: str, base_name: str = None):
        # base_name: str --- None - for with each other; 'EVA', etc. - for others
        if base_name is None:
            res = self._db.rosp_each_to_each(tab)
            math_expectation = self._db.math_expectation_each_to_each(tab)[0]['math_expectation']
            std = self._db.std_each_to_each(tab)[0]['standart_dev']
            mode = self._db.mode_each_to_each(tab)[0]['diff_num']
            min_value = self._db.min_value_each_to_each(tab)[0]['min']
            max_value = self._db.max_value_each_to_each(tab)[0]['max']
            coeff = self._db.coeff_each_to_each(tab)[0]['koef']

        else:
            res = self._db.distribution(base_name, tab)
            math_expectation = self._db.math_expectation(base_name, tab)[0]['math_expectation']
            std = self._db.std(base_name, tab)[0]['standart_dev']
            mode = self._db.mode(base_name, tab)[0]['diff_num']
            min_value = self._db.min_value(base_name, tab)[0]['min']
            max_value = self._db.max_value(base_name, tab)[0]['max']
            coeff = self._db.coeff(base_name, tab)[0]['koef']

        line_1 = [j["frequency"] for i in range(self._dist_range) for j in res if j["diff_num"] == i]
        line_2 = [j["p"] for i in range(self._dist_range) for j in res if j["diff_num"] == i]
        dist_name_1 = f'Розподіл відносно {base_name}' if base_name is not None else 'Розподіл кожен з кожним'
        if base_name is not None:
            dist_name_2 = f'Розподіл відносно {base_name} (частка)'
        else:
            dist_name_2 = 'Розподіл кожен з кожним (частка)'

        self._wrapper.insert_distribution(
            tab,
            {
                dist_name_1: line_1, dist_name_2: line_2,
                'values': {
                    'mean': math_expectation,
                    'std': std,
                    'mode': mode,
                    'min': min_value,
                    'max': max_value,
                    'coeff': coeff
                }
            }
        )

    def build_wild_type_and_poly(self, tab: str):
        wild_type = self._db.select("public.sequence", [f"name='WILD_TYPE_{tab}'"], first_=True)

        poly_eva = self._db.polim('EVA', tab)
        poly_andrews = self._db.polim('ANDREWS', tab)
        poly_wild = self._db.polim('WILD_TYPE_ALL', tab)

        self._wrapper.insert_wild_type(
            tab, wild_type[3], poly_eva[0][0], poly_andrews[0][0], poly_wild[0][0], 0
        )

    def build(self, tab: str = 'ALL'):
        dist_range = [x for x in range(self._dist_range)]
        self._wrapper.insert_distances(tab, dist_range)

        self._db.calculate_wild(tab)
        self._db.commit()

        for base_name in ['EVA', 'ANDREWS', f'WILD_TYPE_{tab}', None]:  # None for 'with each other'
            self.build_distribution(tab, base_name)

        self.build_wild_type_and_poly(tab)


if __name__ == '__main__':
    wrapper_ = xlsx_wrapper.XlsxWrapper('test.xlsx')
    db_ = database.Database(
        psycopg2.connect("dbname='nrbd' user='postgres' host='localhost' password='postgres'"),
        debug=True
    )

    districts = ['ALL', 'IF', 'BK', 'BG', 'ST', 'CH', 'KHM']
    # districts = ['ALL', *db_.get_distinct_regions()] # failed on MODE calculation for 'ANDREWS' + 'B' region

    for tab_ in districts:
        TabBuilder(wrapper_, db_).build(tab_)

    wrapper_.save()
