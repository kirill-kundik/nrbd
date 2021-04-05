import csv

import psycopg2

import database


def read_fasta(filename):
    with open(filename) as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            yield row


def main():
    conn = psycopg2.connect("dbname='nrbd' user='postgres' host='localhost' password='gulayeva'")
    # conn = psycopg2.connect("dbname='nrbd' user='postgres' host='localhost' password='root'")
    # conn.set_session(autocommit=True) # enabling autocommit
    db = database.Database(conn)

    # base_sequence = db.get_base_sequence(1)
    base_sequence = db.insert_sequence(
        'TTCTTTCATGGGGAAGCAGATTTGGGTACCACCCAAGTATTGACTCACCCATCAACAACCGCTATGTATTTCGTACATTACTGCCAGCCACCATGAATATTGTACAGTACCATAAATACTTGACCACCTGTAGTACATAAAAACCCAATCCACATCAAAACCCTCCCCCCATGCTTACAAGCAAGTACAGCAATCAACCTTCAACTGTCACACATCAACTGCAACTCCAAAGCCACCCCTCACCCACTAGGATATCAACAAACCTACCCACCCTTAACAGTACATAGCACATAAAGCCATTTACCGTACATAGCACATTACAGTCAAATCCCTTCTCGTCCCCATGGATGACCCCCCTCAGATAGGGGTCCCTTGAC',
        type_=1,
    )

    fasta = read_fasta('result.csv')
    next(fasta, None)  # skip csv headers

    base_url = 'https://www.ncbi.nlm.nih.gov/nuccore/'

    processed = 0

    for f in fasta:
        sequence = db.get_sequence(f[2])
        if sequence is None:
            sequence = db.insert_sequence(f[2], base_sequence_id=base_sequence['id'])

        region = db.get_region(f[1])
        url = f'{base_url}{f[0]}'
        db.insert_person(region['id'], sequence['id'], url)

        # difference = fasta_comp.compare_fasta(base_sequence['fasta'], f[2])
        #
        # if difference:
        #     db.insert_sequence_differences(list(map(lambda x: [*x, sequence['id']], difference)))

        conn.commit()

        processed += 1
        print(f'\rProcessed: {processed}', end='', flush=True)


if __name__ == '__main__':
    main()
