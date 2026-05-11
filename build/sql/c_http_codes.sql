/* Official HTTP Status Codes *//* https://www.iana.org/assignments/http-status-codes/http-status-codes.xhtml */
create table qbl.http_codes (
   code        number not null
      constraint pk_http_code primary key,
   description varchar2(2000 char) not null,
   code_class  number generated always as ( trunc(code / 100) * 100 ) virtual,
   reference   varchar2(1000 char) default 'INPRS', /*AUDIT FIELDS*/
   updated_at  timestamp default systimestamp not null,
   updated_by  varchar2(50 char) default nvl(
      sys_context(
         'userenv',
         'client_identifier'
      ),
      user
   ) not null,
   created_at  timestamp default systimestamp not null,
   created_by  varchar2(50 char) default nvl(
      sys_context(
         'userenv',
         'client_identifier'
      ),
      user
   ) not null
);declare begin
   savepoint insert_start;
   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '100',
              'Continue',
              '[RFC9110, Section 15.2.1]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '101',
              'Switching Protocols',
              '[RFC9110, Section 15.2.2]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '102',
              'Processing',
              '[RFC2518]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '103',
              'Early Hints',
              '[RFC8297]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '104',
              'Upload Resumption Supported',
              '[draft-ietf-httpbis-resumable-upload-05](TEMPORARY - registered 2024-11-13, extension registered 2025-09-15, expires 2026-11-13)'
              );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '200',
              'OK',
              '[RFC9110, Section 15.3.1]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '201',
              'Created',
              '[RFC9110, Section 15.3.2]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '202',
              'Accepted',
              '[RFC9110, Section 15.3.3]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '203',
              'Non-Authoritative Information',
              '[RFC9110, Section 15.3.4]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '204',
              'No Content',
              '[RFC9110, Section 15.3.5]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '205',
              'Reset Content',
              '[RFC9110, Section 15.3.6]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '206',
              'Partial Content',
              '[RFC9110, Section 15.3.7]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '207',
              'Multi-Status',
              '[RFC4918]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '208',
              'Already Reported',
              '[RFC5842]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '226',
              'IM Used',
              '[RFC3229]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '300',
              'Multiple Choices',
              '[RFC9110, Section 15.4.1]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '301',
              'Moved Permanently',
              '[RFC9110, Section 15.4.2]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '302',
              'Found',
              '[RFC9110, Section 15.4.3]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '303',
              'See Other',
              '[RFC9110, Section 15.4.4]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '304',
              'Not Modified',
              '[RFC9110, Section 15.4.5]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '305',
              'Use Proxy',
              '[RFC9110, Section 15.4.6]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '307',
              'Temporary Redirect',
              '[RFC9110, Section 15.4.8]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '308',
              'Permanent Redirect',
              '[RFC9110, Section 15.4.9]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '400',
              'Bad Request',
              '[RFC9110, Section 15.5.1]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '401',
              'Unauthorized',
              '[RFC9110, Section 15.5.2]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '402',
              'Payment Required',
              '[RFC9110, Section 15.5.3]' );
   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '403',
              'Forbidden',
              '[RFC9110, Section 15.5.4]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '404',
              'Not Found',
              '[RFC9110, Section 15.5.5]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '405',
              'Method Not Allowed',
              '[RFC9110, Section 15.5.6]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '406',
              'Not Acceptable',
              '[RFC9110, Section 15.5.7]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '407',
              'Proxy Authentication Required',
              '[RFC9110, Section 15.5.8]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '408',
              'Request Timeout',
              '[RFC9110, Section 15.5.9]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '409',
              'Conflict',
              '[RFC9110, Section 15.5.10]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '410',
              'Gone',
              '[RFC9110, Section 15.5.11]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '411',
              'Length Required',
              '[RFC9110, Section 15.5.12]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '412',
              'Precondition Failed',
              '[RFC9110, Section 15.5.13]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '413',
              'Content Too Large',
              '[RFC9110, Section 15.5.14]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '414',
              'URI Too Long',
              '[RFC9110, Section 15.5.15]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '415',
              'Unsupported Media Type',
              '[RFC9110, Section 15.5.16]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '416',
              'Range Not Satisfiable',
              '[RFC9110, Section 15.5.17]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '417',
              'Expectation Failed',
              '[RFC9110, Section 15.5.18]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '421',
              'Misdirected Request',
              '[RFC9110, Section 15.5.20]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '422',
              'Unprocessable Content',
              '[RFC9110, Section 15.5.21]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '423',
              'Locked',
              '[RFC4918]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '424',
              'Failed Dependency',
              '[RFC4918]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '425',
              'Too Early',
              '[RFC8470]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '426',
              'Upgrade Required',
              '[RFC9110, Section 15.5.22]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '428',
              'Precondition Required',
              '[RFC6585]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '429',
              'Too Many Requests',
              '[RFC6585]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '431',
              'Request Header Fields Too Large',
              '[RFC6585]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '451',
              'Unavailable For Legal Reasons',
              '[RFC7725]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '500',
              'Internal Server Error',
              '[RFC9110, Section 15.6.1]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '501',
              'Not Implemented',
              '[RFC9110, Section 15.6.2]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '502',
              'Bad Gateway',
              '[RFC9110, Section 15.6.3]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '503',
              'Service Unavailable',
              '[RFC9110, Section 15.6.4]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '504',
              'Gateway Timeout',
              '[RFC9110, Section 15.6.5]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '505',
              'HTTP Version Not Supported',
              '[RFC9110, Section 15.6.6]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '506',
              'Variant Also Negotiates',
              '[RFC2295]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '507',
              'Insufficient Storage',
              '[RFC4918]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '508',
              'Loop Detected',
              '[RFC5842]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '510',
              'Not Extended (OBSOLETED)',
              '[RFC2774][Status change of HTTP experiments to Historic]' );   insert into perf.http_codes columns (
      code,
      description,
      reference
   ) values ( '511',
              'Network Authentication Required',
              '[RFC6585]' );   commit;
exception
   when others then
      rollback to insert_start;
      raise;
end;