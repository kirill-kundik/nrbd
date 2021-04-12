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
    db.insert_sequence(
        'TTCTTTCATGGGGAAGCAGATTTGGGTACCACCCAAGTATTGACTCACCCATCAACAACCGCTATGTATTTCGTACATTACTGCCAGCCACCATGAATATTGTACAGTACCATAAATACTTGACCACCTGTAGTACATAAAAACCCAATCCACATCAAAACCCTCCCCCCATGCTTACAAGCAAGTACAGCAATCAACCTTCAACTGTCACACATCAACTGCAACTCCAAAGCCACCCCTCACCCACTAGGATATCAACAAACCTACCCACCCTTAACAGTACATAGCACATAAAGCCATTTACCGTACATAGCACATTACAGTCAAATCCCTTCTCGTCCCCATGGATGACCCCCCTCAGATAGGGGTCCCTTGAC',
        type_=1, name="EVA"
    )
    db.insert_sequence(
        'TTCTTTCATGGGGAAGCAGATTTGGGTACCACCCAAGTATTGACTCACCCATCAACAACCGCTATGTATTTCGTACATTACTGCCAGCCACCATGAATATTGTACGGTACCATAAATACTTGACCACCTGTAGTACATAAAAACCCAATCCACATCAAAACCCCCTCCCCATGCTTACAAGCAAGTACAGCAATCAACCCTCAACTATCACACATCAACTGCAACTCCAAAGCCACCCCTCACCCACTAGGATACCAACAAACCTACCCACCCTTAACAGTACATAGTACATAAAGCCATTTACCGTACATAGCACATTACAGTCAAATCCCTTCTCGTCCCCATGGATGACCCCCCTCAGATAGGGGTCCCTTGAC',
        type_=1, name="ANDREWS"
    )
    conn.commit()

    fasta = read_fasta('result.csv')
    next(fasta, None)  # skip csv headers

    base_url = 'https://www.ncbi.nlm.nih.gov/nuccore/'

    processed = 0

    for f in fasta:
        sequence = db.get_sequence(f[2], 0)
        if sequence is None:
            sequence = db.insert_sequence(f[2])

        region = db.get_region(f[1])
        url = f'{base_url}{f[0]}'
        db.insert_person(region['id'], sequence['id'], url)

        conn.commit()

        processed += 1
        print(f'\rProcessed: {processed}', end='', flush=True)


if __name__ == '__main__':
    main()
