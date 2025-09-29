-- ============================================================================
-- SISTEMA DE ANÁLISIS DE OPINIONES DE CLIENTES
-- Script de Creación de Base de Datos para SQL Server
-- Archivo: create_database.sql
-- ============================================================================

-- Crear la base de datos
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'CustomerOpinions')
BEGIN
    CREATE DATABASE CustomerOpinions;
END;
GO

-- Usar la base de datos
USE CustomerOpinions;
GO

-- ============================================================================
-- TABLA: CLIENTES
-- Archivo CSV: clients.csv (IdCliente, Nombre, Email)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='clientes' AND xtype='U')
CREATE TABLE clientes (
    IdCliente INT PRIMARY KEY,
    Nombre NVARCHAR(100) NOT NULL,
    Email NVARCHAR(150) NOT NULL UNIQUE,
    fecha_creacion DATETIME DEFAULT GETDATE()
);
GO

-- ============================================================================
-- TABLA: PRODUCTOS  
-- Archivo CSV: products.csv (IdProducto, Nombre, Categoría)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='productos' AND xtype='U')
CREATE TABLE productos (
    IdProducto INT PRIMARY KEY,
    Nombre NVARCHAR(200) NOT NULL,
    Categoria NVARCHAR(100) NOT NULL,
    fecha_creacion DATETIME DEFAULT GETDATE()
);
GO

-- ============================================================================
-- TABLA: FUENTES
-- Archivo CSV: fuente_datos.csv (IdFuente, TipoFuente, FechaCarga)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fuentes' AND xtype='U')
CREATE TABLE fuentes (
    IdFuente NVARCHAR(20) PRIMARY KEY,
    TipoFuente NVARCHAR(50) NOT NULL,
    FechaCarga DATE NOT NULL,
    fecha_creacion DATETIME DEFAULT GETDATE()
);
GO

-- ============================================================================
-- TABLA: ENCUESTAS
-- Archivo CSV: surveys_part1.csv (IdOpinion, IdCliente, IdProducto, Fecha, Comentario, Clasificación, PuntajeSatisfacción, Fuente)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='encuestas' AND xtype='U')
CREATE TABLE encuestas (
    IdOpinion INT PRIMARY KEY,
    IdCliente INT NOT NULL,
    IdProducto INT NOT NULL,
    Fecha DATE NOT NULL,
    Comentario NVARCHAR(MAX),
    Clasificacion NVARCHAR(50),
    PuntajeSatisfaccion TINYINT CHECK (PuntajeSatisfaccion >= 1 AND PuntajeSatisfaccion <= 10),
    FuenteTexto NVARCHAR(100),
    fecha_creacion DATETIME DEFAULT GETDATE(),
    
    -- Claves foráneas
    CONSTRAINT FK_Encuestas_Cliente FOREIGN KEY (IdCliente) REFERENCES clientes(IdCliente) ON DELETE CASCADE,
    CONSTRAINT FK_Encuestas_Producto FOREIGN KEY (IdProducto) REFERENCES productos(IdProducto) ON DELETE CASCADE
);
GO

-- ============================================================================
-- TABLA: COMENTARIOS_SOCIALES
-- Archivo CSV: social_comments.csv (IdComment, IdCliente, IdProducto, Fuente, Fecha, Comentario)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='comentarios_sociales' AND xtype='U')
CREATE TABLE comentarios_sociales (
    IdComment NVARCHAR(20) PRIMARY KEY,
    IdCliente INT NOT NULL,
    IdProducto INT NOT NULL,
    FuenteTexto NVARCHAR(100),
    Fecha DATE NOT NULL,
    Comentario NVARCHAR(MAX),
    fecha_creacion DATETIME DEFAULT GETDATE(),
    
    -- Claves foráneas
    CONSTRAINT FK_Comentarios_Cliente FOREIGN KEY (IdCliente) REFERENCES clientes(IdCliente) ON DELETE CASCADE,
    CONSTRAINT FK_Comentarios_Producto FOREIGN KEY (IdProducto) REFERENCES productos(IdProducto) ON DELETE CASCADE
);
GO

-- ============================================================================
-- TABLA: RESENAS_WEB
-- Archivo CSV: web_reviews.csv (IdReview, IdCliente, IdProducto, Fecha, Comentario, Rating)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='resenas_web' AND xtype='U')
CREATE TABLE resenas_web (
    IdReview NVARCHAR(20) PRIMARY KEY,
    IdCliente INT NOT NULL,
    IdProducto INT NOT NULL,
    Fecha DATE NOT NULL,
    Comentario NVARCHAR(MAX),
    Rating TINYINT CHECK (Rating >= 1 AND Rating <= 5),
    fecha_creacion DATETIME DEFAULT GETDATE(),
    
    -- Claves foráneas
    CONSTRAINT FK_Resenas_Cliente FOREIGN KEY (IdCliente) REFERENCES clientes(IdCliente) ON DELETE CASCADE,
    CONSTRAINT FK_Resenas_Producto FOREIGN KEY (IdProducto) REFERENCES productos(IdProducto) ON DELETE CASCADE
);
GO

-- ============================================================================
-- ÍNDICES PARA OPTIMIZAR CONSULTAS
-- ============================================================================

-- Índices para tabla clientes
CREATE NONCLUSTERED INDEX IX_Clientes_Email ON clientes(Email);
CREATE NONCLUSTERED INDEX IX_Clientes_Nombre ON clientes(Nombre);

-- Índices para tabla productos
CREATE NONCLUSTERED INDEX IX_Productos_Categoria ON productos(Categoria);
CREATE NONCLUSTERED INDEX IX_Productos_Nombre ON productos(Nombre);

-- Índices para tabla encuestas
CREATE NONCLUSTERED INDEX IX_Encuestas_Cliente ON encuestas(IdCliente);
CREATE NONCLUSTERED INDEX IX_Encuestas_Producto ON encuestas(IdProducto);
CREATE NONCLUSTERED INDEX IX_Encuestas_Fecha ON encuestas(Fecha);
CREATE NONCLUSTERED INDEX IX_Encuestas_Clasificacion ON encuestas(Clasificacion);

-- Índices para tabla comentarios_sociales
CREATE NONCLUSTERED INDEX IX_Comentarios_Cliente ON comentarios_sociales(IdCliente);
CREATE NONCLUSTERED INDEX IX_Comentarios_Producto ON comentarios_sociales(IdProducto);
CREATE NONCLUSTERED INDEX IX_Comentarios_Fecha ON comentarios_sociales(Fecha);

-- Índices para tabla resenas_web
CREATE NONCLUSTERED INDEX IX_Resenas_Cliente ON resenas_web(IdCliente);
CREATE NONCLUSTERED INDEX IX_Resenas_Producto ON resenas_web(IdProducto);
CREATE NONCLUSTERED INDEX IX_Resenas_Fecha ON resenas_web(Fecha);
CREATE NONCLUSTERED INDEX IX_Resenas_Rating ON resenas_web(Rating);
GO

-- ============================================================================
-- VISTAS PARA ANÁLISIS
-- ============================================================================

-- Vista consolidada de todas las opiniones
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vista_opiniones_consolidadas')
    DROP VIEW vista_opiniones_consolidadas;
GO

CREATE VIEW vista_opiniones_consolidadas AS
SELECT 
    'Encuesta' as tipo_opinion,
    CAST(IdOpinion AS NVARCHAR(20)) as id_opinion,
    IdCliente,
    IdProducto,
    Fecha,
    Comentario,
    Clasificacion as sentimiento,
    PuntajeSatisfaccion as puntuacion,
    FuenteTexto as fuente
FROM encuestas
WHERE Comentario IS NOT NULL

UNION ALL

SELECT 
    'Comentario Social' as tipo_opinion,
    IdComment as id_opinion,
    IdCliente,
    IdProducto,
    Fecha,
    Comentario,
    NULL as sentimiento,
    NULL as puntuacion,
    FuenteTexto as fuente
FROM comentarios_sociales
WHERE Comentario IS NOT NULL

UNION ALL

SELECT 
    'Reseña Web' as tipo_opinion,
    IdReview as id_opinion,
    IdCliente,
    IdProducto,
    Fecha,
    Comentario,
    NULL as sentimiento,
    Rating as puntuacion,
    'Web' as fuente
FROM resenas_web
WHERE Comentario IS NOT NULL;
GO

-- Vista de estadísticas por producto
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vista_estadisticas_producto')
    DROP VIEW vista_estadisticas_producto;
GO

CREATE VIEW vista_estadisticas_producto AS
SELECT 
    p.IdProducto,
    p.Nombre,
    p.Categoria,
    COUNT(DISTINCT e.IdCliente) + 
    COUNT(DISTINCT cs.IdCliente) + 
    COUNT(DISTINCT rw.IdCliente) as total_clientes_opinaron,
    COUNT(e.IdOpinion) as total_encuestas,
    COUNT(cs.IdComment) as total_comentarios_sociales,
    COUNT(rw.IdReview) as total_resenas_web,
    AVG(CAST(e.PuntajeSatisfaccion AS FLOAT)) as promedio_satisfaccion_encuestas,
    AVG(CAST(rw.Rating AS FLOAT)) as promedio_rating_web
FROM productos p
LEFT JOIN encuestas e ON p.IdProducto = e.IdProducto
LEFT JOIN comentarios_sociales cs ON p.IdProducto = cs.IdProducto
LEFT JOIN resenas_web rw ON p.IdProducto = rw.IdProducto
GROUP BY p.IdProducto, p.Nombre, p.Categoria;
GO

-- ============================================================================
-- CONSULTA DE VERIFICACIÓN INICIAL
-- ============================================================================

-- Verificar que todas las tablas fueron creadas
SELECT 
    TABLE_NAME as 'Tabla Creada',
    TABLE_TYPE as 'Tipo'
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_CATALOG = 'CustomerOpinions'
ORDER BY TABLE_NAME;
