/* Official HTTP Status Codes */
/* https://www.iana.org/assignments/http-status-codes/http-status-codes.xhtml */
CREATE TABLE qbl.http_codes (
   code        NUMBER NOT NULL CONSTRAINT pk_http_code PRIMARY KEY,
   description VARCHAR2(2000 CHAR) NOT NULL,
   code_class  NUMBER GENERATED ALWAYS AS ( TRUNC(code / 100) * 100 ) VIRTUAL,
   reference   VARCHAR2(1000 CHAR) DEFAULT 'INPRS',
   updated_at  TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
   updated_by  VARCHAR2(50 CHAR) DEFAULT NVL(SYS_CONTEXT('userenv','client_identifier'), USER) NOT NULL,
   created_at  TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
   created_by  VARCHAR2(50 CHAR) DEFAULT NVL(SYS_CONTEXT('userenv','client_identifier'), USER) NOT NULL
);

BEGIN
   SAVEPOINT insert_start;

   INSERT INTO qbl.http_codes (code, description, reference) VALUES (100, 'Continue',                        '[RFC9110, Section 15.2.1]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (101, 'Switching Protocols',             '[RFC9110, Section 15.2.2]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (102, 'Processing',                      '[RFC2518]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (103, 'Early Hints',                     '[RFC8297]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (104, 'Upload Resumption Supported',     '[draft-ietf-httpbis-resumable-upload-05]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (200, 'OK',                              '[RFC9110, Section 15.3.1]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (201, 'Created',                        '[RFC9110, Section 15.3.2]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (202, 'Accepted',                       '[RFC9110, Section 15.3.3]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (203, 'Non-Authoritative Information',  '[RFC9110, Section 15.3.4]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (204, 'No Content',                     '[RFC9110, Section 15.3.5]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (205, 'Reset Content',                  '[RFC9110, Section 15.3.6]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (206, 'Partial Content',                '[RFC9110, Section 15.3.7]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (207, 'Multi-Status',                   '[RFC4918]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (208, 'Already Reported',               '[RFC5842]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (226, 'IM Used',                        '[RFC3229]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (300, 'Multiple Choices',               '[RFC9110, Section 15.4.1]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (301, 'Moved Permanently',              '[RFC9110, Section 15.4.2]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (302, 'Found',                          '[RFC9110, Section 15.4.3]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (303, 'See Other',                      '[RFC9110, Section 15.4.4]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (304, 'Not Modified',                   '[RFC9110, Section 15.4.5]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (305, 'Use Proxy',                      '[RFC9110, Section 15.4.6]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (307, 'Temporary Redirect',             '[RFC9110, Section 15.4.8]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (308, 'Permanent Redirect',             '[RFC9110, Section 15.4.9]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (400, 'Bad Request',                    '[RFC9110, Section 15.5.1]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (401, 'Unauthorized',                   '[RFC9110, Section 15.5.2]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (402, 'Payment Required',               '[RFC9110, Section 15.5.3]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (403, 'Forbidden',                      '[RFC9110, Section 15.5.4]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (404, 'Not Found',                      '[RFC9110, Section 15.5.5]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (405, 'Method Not Allowed',             '[RFC9110, Section 15.5.6]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (406, 'Not Acceptable',                 '[RFC9110, Section 15.5.7]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (407, 'Proxy Authentication Required',  '[RFC9110, Section 15.5.8]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (408, 'Request Timeout',                '[RFC9110, Section 15.5.9]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (409, 'Conflict',                       '[RFC9110, Section 15.5.10]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (410, 'Gone',                           '[RFC9110, Section 15.5.11]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (411, 'Length Required',                '[RFC9110, Section 15.5.12]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (412, 'Precondition Failed',            '[RFC9110, Section 15.5.13]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (413, 'Content Too Large',              '[RFC9110, Section 15.5.14]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (414, 'URI Too Long',                   '[RFC9110, Section 15.5.15]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (415, 'Unsupported Media Type',         '[RFC9110, Section 15.5.16]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (416, 'Range Not Satisfiable',          '[RFC9110, Section 15.5.17]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (417, 'Expectation Failed',             '[RFC9110, Section 15.5.18]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (421, 'Misdirected Request',            '[RFC9110, Section 15.5.20]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (422, 'Unprocessable Content',          '[RFC9110, Section 15.5.21]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (423, 'Locked',                         '[RFC4918]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (424, 'Failed Dependency',              '[RFC4918]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (425, 'Too Early',                      '[RFC8470]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (426, 'Upgrade Required',               '[RFC9110, Section 15.5.22]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (428, 'Precondition Required',          '[RFC6585]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (429, 'Too Many Requests',              '[RFC6585]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (431, 'Request Header Fields Too Large','[RFC6585]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (451, 'Unavailable For Legal Reasons',  '[RFC7725]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (500, 'Internal Server Error',          '[RFC9110, Section 15.6.1]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (501, 'Not Implemented',                '[RFC9110, Section 15.6.2]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (502, 'Bad Gateway',                    '[RFC9110, Section 15.6.3]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (503, 'Service Unavailable',            '[RFC9110, Section 15.6.4]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (504, 'Gateway Timeout',                '[RFC9110, Section 15.6.5]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (505, 'HTTP Version Not Supported',     '[RFC9110, Section 15.6.6]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (506, 'Variant Also Negotiates',        '[RFC2295]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (507, 'Insufficient Storage',           '[RFC4918]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (508, 'Loop Detected',                  '[RFC5842]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (510, 'Not Extended (OBSOLETED)',       '[RFC2774]');
   INSERT INTO qbl.http_codes (code, description, reference) VALUES (511, 'Network Authentication Required','[RFC6585]');

   COMMIT;
EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK TO insert_start;
      RAISE;
END;
/
