def compare_fasta(base_fasta, target_fasta):
    return [
        (index, target)
        for index, (base, target)
        in enumerate(zip(base_fasta, target_fasta))
        if target != base
    ]


if __name__ == '__main__':
    base_f = 'AAAAA'
    target_f = 'AAABC'

    print(compare_fasta(base_f, target_f))
