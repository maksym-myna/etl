work = """
    SELECT 
        w.work_id,
        s.subject_id,
        w.publisher_id,
        w.title, 
        w.release_year,
        w.weight
    FROM
        WORK w
    LEFT JOIN (
        SELECT 
            work_id,
            MIN(subject_id) AS subject_id
        from
            work_subject
        GROUP BY 
            work_id
    ) s USING (work_id)
    WHERE
        w.modified_at > '{start_date}'
    AND
    (
        EXISTS (SELECT 1 FROM loan JOIN inventory_item ii USING (item_id) WHERE ii.work_id = w.work_id)
        OR EXISTS (SELECT 1 FROM rating r WHERE r.work_id = w.work_id)
        OR EXISTS (SELECT 1 FROM listing l WHERE l.work_id = w.work_id)
    )
"""

publisher = """
    SELECT
        publisher_id,
        publisher_name
    FROM
        publisher p
    WHERE
        p.modified_at > '{start_date}'
    AND
    (
        EXISTS (SELECT 1 FROM loan JOIN inventory_item ii USING (item_id) join work w using(work_id) WHERE w.publisher_id = p.publisher_id)
        OR EXISTS (SELECT 1 FROM rating r join work w using(work_id) WHERE p.publisher_id = w.publisher_id)
        OR EXISTS (SELECT 1 FROM listing l join work w using(work_id) WHERE p.publisher_id = w.publisher_id)
    )

"""
author = """
    SELECT
        author_id,
        full_name as author_name
    FROM
        author a
    WHERE
        a.modified_at > '{start_date}'
    AND
    (
        EXISTS (SELECT 1 FROM loan JOIN inventory_item ii USING (item_id) join work_author wa using(work_id) WHERE wa.author_id = a.author_id)
        OR EXISTS (SELECT 1 FROM rating r join work w using(work_id) join work_author wa using(work_id) WHERE wa.author_id = a.author_id)
        OR EXISTS (SELECT 1 FROM listing l join work w using(work_id) join work_author wa using(work_id) WHERE wa.author_id = a.author_id)
    )

"""

work_author = """
    SELECT 
        w.work_id, 
        wa.author_id, 
        1.0/count(*) over (partition by w.work_id) as coefficient 
    FROM 
        WORK w
    JOIN 
        work_author wa
    USING
        (work_id)
    WHERE 
        added_at > '{start_date}'
    AND
    (
        EXISTS (SELECT 1 FROM loan JOIN inventory_item ii USING (item_id) WHERE ii.work_id = w.work_id)
        OR EXISTS (SELECT 1 FROM rating r WHERE r.work_id = w.work_id)
        OR EXISTS (SELECT 1 FROM listing l WHERE l.work_id = w.work_id)
    )

"""

subject = """
    select 
        subject_id,
        subject_name
    from 
        subject
    where
        modified_at > '{start_date}'

"""

language = """
    select 
        language_id, lang_name as language_name, speakers
    from
        lang
    WHERE 
        modified_at > '{start_date}'

"""

date = """
    SELECT
        TO_CHAR(date, 'YYYY')::INTEGER AS year,
        TO_CHAR(date, 'FMMonth YYYY') AS month, 
        CONCAT('Q', EXTRACT(QUARTER FROM date), ' ', TO_CHAR(date, 'YYYY')) AS quarter, 
        date::date AS date, 
        TO_CHAR(date, 'YYYYMMDD')::INTEGER AS date_id
    FROM (
        SELECT loaned_at::date as date FROM loan WHERE loaned_at > '{start_date}'
        union
        SELECT rated_at::date as date FROM rating  WHERE rated_at > '{start_date}'
        union
        SELECT listed_at::date as date FROM listing WHERE listed_at > '{start_date}'
    ) x

"""

medium = """
SELECT
    enumsortorder as medium_id,
    initcap(replace(enumlabel, '_', ' ')) as medium_name
FROM
    pg_enum
WHERE
    enumtypid = 'item_medium_type'::regtype
"""

listing_type = """
    SELECT
        enumsortorder as listing_type_id,
        initcap(replace(enumlabel, '_', ' ')) as listing_type_name
    FROM
        pg_enum
    WHERE 
        enumtypid = 'reading_status_type'::regtype
"""

return_fact = """
SELECT DISTINCT ON (work_id, user_id, date_id) 
    pages,
    items_left,
    days_loaned,
    work_age,
    reader_age,
    user_id,
    date_id,
    work_id,
    medium_id,
    language_id
FROM 
    (
        SELECT
            work.pages,
            EXTRACT('Day' FROM returned_at - loaned_at) AS days_loaned,
            EXTRACT(YEAR FROM CURRENT_DATE) - release_year AS work_age,
            EXTRACT(YEAR FROM AGE(now(), birthday))::INTEGER AS reader_age,
            library_user.user_id, TO_CHAR(returned_at, 'YYYYMMDD')::INTEGER AS date_id,
            work.work_id,
            enumsortorder AS medium_id,
            work.language_id,
            loaned_at,
            (  
                SELECT
                    CASE 
                        WHEN 
                            medium IN ('EBOOK', 'AUDIOBOOK')
                        THEN 1
                    ELSE 
                        (COUNT(DISTINCT ii.item_id) - COUNT(CASE WHEN loaned_at IS NULL THEN 1 END) + COUNT(returned_at) - COUNT(loaned_at))
                    END AS items_left
                FROM
                    inventory_item ii
                LEFT JOIN
                    loan l
                    ON 
                        l.item_id = ii.item_id
                    AND
                        loaned_at <= l1.loaned_at
                LEFT JOIN
                    loan_return lr
                    ON
                        lr.loan_id = l.loan_id
                    AND
                        returned_at <= l1.loaned_at
                WHERE
                    ii.work_id = work.work_id
            )
        FROM
            loan_return
        JOIN
            loan l1
        USING
            (loan_id)
        JOIN
            inventory_item
        USING
            (item_id)
        JOIN
            work
        USING
            (work_id)
        JOIN
            library_user
        USING
            (user_id)
        JOIN
            pg_enum
        ON
            enumlabel = medium::text
        WHERE
            loaned_at > '{start_date}'
    ) AS subquery
WHERE
    items_left >= 0
"""

rating_fact = """
    select distinct on (work_id, user_id)
        pages,
        score,
        EXTRACT(YEAR FROM CURRENT_DATE) - release_year as work_age,
        EXTRACT(YEAR FROM AGE(now(), birthday))::INTEGER as reader_age,
        user_id,
        TO_CHAR(rated_at, 'YYYYMMDD')::INTEGER as date_id,
        work_id,
        language_id
    from
        rating 
    join 
        work
    using
        (work_id)
    join
        library_user
    using 
        (user_id)
    where 
        rated_at > '{start_date}'
    and 
        birthday < rated_at
"""

listing_fact = """
    select distinct on (work_id, user_id, listing_type_id)
        pages,
        EXTRACT(YEAR FROM CURRENT_DATE) - release_year as work_age,
        EXTRACT(YEAR FROM AGE(now(), birthday))::INTEGER as reader_age,
        user_id,
        TO_CHAR(listed_at, 'YYYYMMDD')::INTEGER as date_id,
        work_id,
        language_id,
        enumsortorder as listing_type_id
    from
        listing 
    join 
        pg_enum
    on 
        enumlabel = reading_status::text 
    join 
        work
    using
        (work_id)
    join
        library_user
    using 
        (user_id)
    where 
        listed_at > '{start_date}'
    and 
        birthday < listed_at
"""

user = """
WITH age_calculation AS (
    SELECT 
        user_id,
        EXTRACT(YEAR FROM AGE(birthday)) as age,
        gender,
        first_name,
        last_name
    FROM 
        library_user 
    WHERE 
        modified_at > '{start_date}'
)
SELECT 
    user_id,
    CASE 
        WHEN age BETWEEN 0 AND 12 THEN '0-12'
        WHEN age BETWEEN 13 AND 19 THEN '13-19'
        WHEN age BETWEEN 20 AND 29 THEN '20-29'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 49 THEN '40-49'
        ELSE '50+'
    END as age_group,
    CASE 
        WHEN gender = 'f' THEN 'female'
        WHEN gender = 'm' THEN 'male'
        WHEN gender = 'n' THEN 'non-binary'
        ELSE gender
    END as gender,
    first_name,
    CONCAT(first_name, ' ', last_name) as full_name
FROM 
    age_calculation
"""
