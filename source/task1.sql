SELECT 
    c.id AS course_id,
    c.name AS course_name,
    s.name AS subject_name,
    s.project AS subject_type,
    ct.name AS course_type,
    c.starts_at::date AS course_start_date,
    cu.user_id AS student_id,
    u.last_name AS student_last_name,
    ci.name AS student_city,
    cu.active AS is_student_active,
    cu.created_at::date AS course_open_date,
    FLOOR(cu.available_lessons / c.lessons_in_month)::int4 AS months_course_open,
    coalesce(h.cnt, 0) AS completed_homework_count
FROM
    courses c
JOIN 
    subjects s ON c.subject_id = s.id
JOIN 
    course_types ct ON c.course_type_id = ct.id
    	AND ct.id IN (1, 6) --годовые курсы (не нашёл атрибута для разделения на ЕГЭ, ОГЭ и др.)
JOIN 
    course_users cu ON c.id = cu.course_id
JOIN 
    users u ON cu.user_id = u.id
        and u.user_role_id = 5 --отбираем только студентов
LEFT JOIN 
    cities ci ON u.city_id = ci.id
LEFT JOIN
	(
	SELECT 
		hd.user_id
		,l.course_id
		,count(DISTINCT hd.homework_id) cnt
	FROM homework_done hd
		JOIN homework_lessons hl ON hl.homework_id = hd.homework_id
		JOIN lessons l ON l.id = hl.lesson_id
	GROUP BY 1, 2
	) h ON h.user_id = u.id AND h.course_id = c.id -- в этом подзапросе получаем количество уникальных сданных ДЗ, группируя по пользователю и курсу
ORDER BY 
    completed_homework_count, c.id, cu.user_id