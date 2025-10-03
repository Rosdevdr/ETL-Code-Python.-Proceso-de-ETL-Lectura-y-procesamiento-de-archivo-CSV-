USE CustomerOpinions;
GO

SELECT 'clientes' AS tabla, COUNT(*) AS total FROM clientes
UNION ALL
SELECT 'productos', COUNT(*) FROM productos
UNION ALL
SELECT 'fuentes', COUNT(*) FROM fuentes
UNION ALL
SELECT 'encuestas', COUNT(*) FROM encuestas
UNION ALL
SELECT 'comentarios_sociales', COUNT(*) FROM comentarios_sociales
UNION ALL
SELECT 'resenas_web', COUNT(*) FROM resenas_web;
