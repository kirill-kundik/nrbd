import csv

import psycopg2

import database
import fasta_comp


def read_fasta(filename):
    with open(filename) as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            yield row


def main():
    conn = psycopg2.connect("dbname='nrbd' user='postgres' host='localhost' password='gulayeva'")
    # conn.set_session(autocommit=True) # enabling autocommit
    db = database.Database(conn)

    base_sequence = db.get_base_sequence(1)

    fasta = read_fasta('result.csv')
    next(fasta, None)  # skip csv headers

    base_url = 'https://www.ncbi.nlm.nih.gov/nuccore/'

    processed = 0

    for f in fasta:
        sequence = db.insert_sequence(f'{base_url}{f[0]}', base_sequence=base_sequence['id'])

        region = db.get_region(f[1])

        db.insert_person(region['id'], sequence['id'])

        difference = fasta_comp.compare_fasta(base_sequence['fasta'], f[2])

        if difference:
            db.insert_sequence_differences(list(map(lambda x: [*x, sequence['id']], difference)))

        conn.commit()

        processed += 1
        print(f'Processed: {processed}', end='')


if __name__ == '__main__':
    main()
