
-- the lack of a PK here is debatable.  did not make 'dt' PK because of
-- potential for future threading
CREATE TABLE logs (
    dt          TIMESTAMP,
    job         VARCHAR(255),
    level       VARCHAR(255),
    msg         VARCHAR(65535)
)
