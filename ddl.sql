DROP FUNCTION IF EXISTS generate_positions();
DROP TRIGGER IF EXISTS generate_fasta_positions ON sequence;

CREATE FUNCTION generate_positions() RETURNS trigger AS
$generate_positions$
BEGIN
    INSERT INTO fasta_position (position, value, sequence_id)
    SELECT ordinality, a, NEW.id
    FROM unnest(regexp_split_to_array(NEW.fasta, '')) WITH ORDINALITY a;
    RETURN NEW;
END
$generate_positions$ LANGUAGE plpgsql;

create table region
(
    id   serial       not null
        constraint region_pkey
            primary key,
    name varchar(255) not null
);

alter table region
    owner to postgres;

create table sequence
(
    id            serial            not null
        constraint sequence_pkey
            primary key,
    sequence_type integer default 0 not null,
    name          varchar(255)      null unique,
    fasta         varchar(377)      not null unique
);

alter table sequence
    owner to postgres;

create trigger generate_fasta_positions
    after insert
    on sequence
    for each row
execute procedure generate_positions();

create table fasta_position
(
    id          serial     not null
        constraint fasta_position_pkey
            primary key,
    position    integer    not null,
    value       varchar(1) not null,
    sequence_id integer    not null
        constraint fasta_position_sequence_id_fkey
            references sequence
            on delete cascade
);

alter table fasta_position
    owner to postgres;

create table person
(
    id          serial  not null
        constraint person_pkey
            primary key,
    region_id   integer
        constraint person_region_id_fkey
            references region
            on delete set null,
    url         character varying(255) unique,
    sequence_id integer not null
        constraint person_sequence_id_fkey
            references sequence
            on delete cascade
);

alter table person
    owner to postgres;

-- CREATE TRIGGER generate_fasta_positions
--     AFTER INSERT
--     ON sequence
--     FOR EACH ROW
-- EXECUTE PROCEDURE generate_positions();
