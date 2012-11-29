#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <glob.h>
#include <errno.h>
#include <stdarg.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <unicode/uloc.h>
#include <unicode/utypes.h>
#include <unicode/ustring.h>
#include "uthash.h"
#include "utstring.h"
#include "json/json.h"
#include <signal.h>
#include <time.h>
#include <event.h>
#include <evhttp.h>
#include <pthread.h>

#define NAME "autocomplete"
#define VERSION "0.3"
#define DEBUG 1
#define DEFAULT_PORT 8080
#define EMPTY_STRING ""
#define KEY_LEN(k) (k->len[0] + k->len[1] + 1)

/*
 *  N.B. These defines directly reference stack variables.
 */
#define key_match(x) (strncmp(((struct el*)x)->ckey->key,ckey->key,strlen(ckey->key)) == 0)
#define key_id_match(x) (key_match(x) && strcmp(((struct el*)x)->ckey->id,ckey->id) == 0)
#define dirty_match(x) (((struct namespace*)x)->dirty > 0)

#define safe_strdup(s) (s ? strdup(s) : NULL)
#define safe_free(s)    \
if (s) {                \
    free(s);            \
}

#if defined __APPLE__ && defined __MACH__
#define GLOBC(g) g.gl_matchc
#else
#define GLOBC(g) g.gl_pathc
#endif

typedef struct composite_key {
    char *key;
    char *id;
    int len[2];
    char data[1];
} composite_key;

typedef struct el {
    composite_key *ckey;
    char *data;
    time_t when;
    int count;
    UT_hash_handle hh;  /* handle for key hash */
    UT_hash_handle rh;  /* handle for results hash */
} el;

struct namespace {
    char *name;
    int nelems;
    int dirty;
    pthread_mutex_t lock;
    struct el *elems;
    UT_hash_handle hh;  /* handle for key hash */
    UT_hash_handle dh;  /* handle for dirty hash */
};

struct evhttp *httpd;
struct namespace *spaces = NULL;
struct event backup_timer;
struct timeval backup_tv = {60, 0};
static pthread_mutex_t master_lock;
static pthread_cond_t backup_cond;
char *default_locale = ULOC_US;
char *db_dir = NULL;
int max_elems = 1000;
int is_running = 1;

void load_namespace(char *namespace);
void put_cb(struct evhttp_request *req, void *arg);
void search_cb(struct evhttp_request *req, void *arg);
void del_cb(struct evhttp_request *req, void *arg);


uint16_t crc16(const uint8_t *buffer, int size) {
    uint16_t crc = 0xFFFF;
    
    while (size--) {
        crc = (crc >> 8) | (crc << 8); 
        crc ^= *buffer++;
        crc ^= ((unsigned char) crc) >> 4;
        crc ^= crc << 12; 
        crc ^= (crc & 0xFF) << 5;
    }   
    return crc;
}

char *utstring_varappend(UT_string *ustr, ...)
{
    va_list argp;
    char *s;
    
    va_start(argp, ustr);
    while ((s = va_arg(argp, char *)) != NULL) {
        utstring_bincpy(ustr, s, strlen(s));
    }
    va_end(argp);
    return utstring_body(ustr);
}

char *namespace_path(UT_string *path, char *namespace)
{
    char buf[7];
    union {
        uint16_t i;
        uint8_t s[2];
    } crc;
    
    crc.i = crc16((const uint8_t *)namespace, strlen(namespace));
    sprintf(buf, "/%hx/%hx/", crc.s[0], crc.s[1]);
    utstring_varappend(path, db_dir, buf, namespace, NULL);
    return utstring_body(path);
}

void make_nested_dirs()
{
    int i, j;
    char a[4], b[4];
    UT_string *path;
    
    utstring_new(path);
    for (i=0; i<256; i++) {
        sprintf(a, "/%hx", i);
        utstring_clear(path);
        utstring_varappend(path, db_dir, a, NULL);
        if (mkdir(utstring_body(path), 0770) != 0 && errno != EEXIST) {
            fprintf(stderr, "mkdir(%s) failed: %s\n", utstring_body(path), strerror(errno));
            exit(1);
        }
        for (j=0; j<256; j++) {
            sprintf(b, "/%hx", j);
            utstring_clear(path);
            utstring_varappend(path, db_dir, a, b, NULL);
            if (mkdir(utstring_body(path), 0770) != 0 && errno != EEXIST) {
                fprintf(stderr, "mkdir(%s) failed: %s\n", utstring_body(path), strerror(errno));
                exit(1);
            }
        }
    }
    utstring_free(path);
}

struct namespace *get_namespace(char *namespace)
{
    struct namespace *ns = NULL;
    pthread_mutex_lock(&master_lock);
    HASH_FIND_STR(spaces, namespace, ns);
    pthread_mutex_unlock(&master_lock);
    return ns;
}

struct namespace *create_namespace(char *namespace, int *new)
{
    struct namespace *ns = NULL;

    if (new) {
        *new = 0;
    }
    HASH_FIND_STR(spaces, namespace, ns);
    if (!ns) {
        /*
         *  This works because only one thread is ever adding namespaces.
         */
        if (new) {
            *new = 1;
        }
        ns = malloc(sizeof(*ns));
        memset(ns, 0, sizeof(*ns));
        ns->name = strdup(namespace);
        pthread_mutex_init(&ns->lock, NULL);
        pthread_mutex_lock(&master_lock);
        HASH_ADD_KEYPTR(hh, spaces, ns->name, strlen(ns->name), ns);
        pthread_mutex_unlock(&master_lock);
        load_namespace(namespace);
    }
    return ns;
}

int time_count_sort(el *a, el *b) {
    if (a->when > b->when) {
        return -1;
    } else if (a->when < b->when) {
        return 1;
    } else {
        if (a->count > b->count) {
            return -1;
        } else if (a->count < b->count) {
            return 1;
        }
    }
    return 0;
}

void free_el(struct el *e)
{
    if (e) {
        safe_free(e->data);
        safe_free(e->ckey);
        free(e);
    }
}

char *utf8_tolower(char *s, char *locale)
{
    UChar *buf = NULL;
    char *buf2 = NULL;
    UErrorCode err = U_ZERO_ERROR;
    int32_t len, len2;

    u_strFromUTF8(NULL, 0, &len, s, -1, &err);
    buf = malloc(sizeof(UChar) * len+1);
    err = U_ZERO_ERROR;
    u_strFromUTF8(buf, len+1, &len, s, -1, &err);
    if (U_FAILURE(err)) {
        fprintf(stderr, "u_strFromUTF8 failed: %s: %s\n", s, u_errorName(err));
        free(buf);
        return NULL;
    }

    err = U_ZERO_ERROR;
    len2 = u_strToLower(NULL, 0, (UChar *)buf, -1, locale, &err);
    if (len2 > len) {
        buf = realloc(buf, len2+1);
    }
    err = U_ZERO_ERROR;
    u_strToLower(buf, len2+1, (UChar *)buf, -1, locale, &err);
    if (U_FAILURE(err)) {
        fprintf(stderr, "u_strToLower failed: %s: %s\n", s, u_errorName(err));
        free(buf);
        return NULL;
    }

    err = U_ZERO_ERROR;
    u_strToUTF8(NULL, 0, &len, buf, -1, &err);
    buf2 = malloc(sizeof(char) * len+1);
    err = U_ZERO_ERROR;
    u_strToUTF8(buf2, len+1, &len, (UChar *)buf, -1, &err);
    if (U_FAILURE(err)) {
        fprintf(stderr, "u_strToUTF8 failed: %s: %s\n", s, u_errorName(err));
        free(buf);
        free(buf2);
        return NULL;
    }
    free(buf);
    return buf2;
}

composite_key *make_key(char *locale, char *key, char *id)
{
    composite_key *ckey = NULL;
    char *normalized_key;
    int klen, ilen;
    
    if (!key) {
        key = EMPTY_STRING;
    }
    if (!id) {
        id = EMPTY_STRING;
    }
    normalized_key = utf8_tolower(key, locale);
    if (normalized_key) {
        klen = strlen(normalized_key);
        ilen = strlen(id);
        ckey = malloc(sizeof(*ckey) + klen + ilen + 1);
        ckey->key = ckey->data;
        strncpy(ckey->key, normalized_key, klen+1);
        ckey->id = ckey->data + klen + 1;
        strncpy(ckey->id, id, ilen+1);
        ckey->len[0] = klen;
        ckey->len[1] = ilen;
        safe_free(normalized_key);
    }
    return ckey;
}

void print_el(struct el *e)
{
    fprintf(stderr, "%s:%s\n\tdata %s\n\twhen %ld\n\tcount %d\n", e->ckey->key,
            e->ckey->id, e->data, e->when, e->count);
}

struct el *put_el(char *namespace, char *locale, char *key, char *id, char *data, time_t when, int mark)
{
    struct namespace *ns;
    struct el *e = NULL;
    composite_key *ckey;
    int new;

    ckey = make_key(locale, key, id);
    if (!ckey) {
        return NULL;
    }
    ns = create_namespace(namespace, &new);
    pthread_mutex_lock(&ns->lock);
    if (HASH_COUNT(ns->elems) > max_elems) {
        /*
         *  This is tricky. UT_hash keeps two sort orders.
         *  This deletes from the head ( oldest insert ).
         */
        e = ns->elems;
        HASH_DEL(ns->elems, e);
        free_el(e);
    }
    HASH_FIND(hh, ns->elems, ckey->data, KEY_LEN(ckey), e);
    if (e) {
        HASH_DEL(ns->elems, e);
        safe_free(ckey);
    } else {
        e = malloc(sizeof(*e));
        memset(e, 0, sizeof(*e));
        e->ckey = ckey;
    }
    safe_free(e->data);
    e->data = (data ? strdup(data) : NULL);
    e->when = when;
    HASH_ADD_KEYPTR(hh, ns->elems, e->ckey->data, KEY_LEN(e->ckey), e);
    if (mark) {
        ns->dirty += 1;
    }
    pthread_mutex_unlock(&ns->lock);

    return e;
}

void load_namespace(char *namespace)
{
    struct el *e;
    UT_string *ustr;
    int fd, n, klen, dlen, ilen;
    char *key = NULL, *id = NULL, *data = NULL;
    struct hdr {
        uint32_t klen;
        uint32_t ilen;
        uint32_t dlen;
        uint32_t when;
        uint32_t count;
    } hdr;
    
    if (!db_dir || !namespace) {
        return;
    }
    
    utstring_new(ustr);
    namespace_path(ustr, namespace);
    fd = open(utstring_body(ustr), O_RDONLY);
    if (fd == -1) {
        fprintf(stderr, "open() failed: %s: %s\n", utstring_body(ustr), strerror(errno));
        utstring_free(ustr);
        return;
    }
    n = read(fd, &hdr, sizeof(hdr));
    fprintf(stderr, "loading: %s from %s\n", namespace, utstring_body(ustr));
    while (n == sizeof(hdr)) {
        klen = ntohl(hdr.klen);
        ilen = ntohl(hdr.ilen);
        dlen = ntohl(hdr.dlen);
        key = realloc(key, klen);
        id = realloc(id, ilen);
        data = realloc(data, dlen);
        n = read(fd, key, klen);
        n = read(fd, id, ilen);
        n = read(fd, data, dlen);
        e = put_el(namespace, NULL, key, id, data, ntohl(hdr.when), 0);
        e->count = ntohl(hdr.count);
        n = read(fd, &hdr, sizeof(hdr));
    }
    close(fd);
    safe_free(key);
    safe_free(id);
    safe_free(data);
    utstring_free(ustr);
}

void save_namespace(struct namespace *ns)
{
    struct el *e;
    UT_string *path1, *path2;
    int fd, ok, n;
    struct hdr {
        uint32_t klen;
        uint32_t ilen;
        uint32_t dlen;
        uint32_t when;
        uint32_t count;
    } hdr;
    
    if (!db_dir || !ns) {
        return;
    }
    fprintf(stderr, "save_namespace %s %d\n", ns->name, ns->dirty);
    
    utstring_new(path1);
    namespace_path(path1, ns->name);
    utstring_bincpy(path1, ".tmp", 5);
    fd = open(utstring_body(path1), O_CREAT|O_TRUNC|O_RDWR, 0660);
    if (fd == -1) {
        fprintf(stderr, "open failed: %s: %s\n", utstring_body(path1), strerror(errno));
        utstring_free(path1);
        return;
    }
    
    pthread_mutex_lock(&ns->lock);
    for (ok=1, e=ns->elems; e != NULL; e=e->hh.next) {
        hdr.klen = htonl(e->ckey->len[0]+1);
        hdr.ilen = htonl(e->ckey->len[1]+1);
        if (e->data != NULL) {
            hdr.dlen = htonl(strlen(e->data)+1);
        } else {
            hdr.dlen = 0;
        }        
        hdr.when = htonl(e->when);
        hdr.count = htonl(e->count);
        n = write(fd, &hdr, sizeof(hdr));
        if (n == -1) {
            fprintf(stderr, "write failed: %s: %s\n", utstring_body(path1), strerror(errno));
            ok = 0;
            break;
        }
        write(fd, e->ckey->key, e->ckey->len[0]+1);
        write(fd, e->ckey->id, e->ckey->len[1]+1);
        if (e->data != NULL) {
            write(fd, e->data, strlen(e->data)+1);
        }
    }
    pthread_mutex_unlock(&ns->lock);

    utstring_new(path2);
    namespace_path(path2, ns->name);
    if (ok) {
        rename(utstring_body(path1), utstring_body(path2));
        ns->dirty = 0;
    }
    close(fd);
    utstring_free(path1);
    utstring_free(path2);
}

void save_namespaces()
{
    struct namespace *ns, *results = NULL;
    int i;
    
    pthread_mutex_lock(&master_lock);
    HASH_SELECT(dh, results, hh, spaces, dirty_match);
    pthread_mutex_unlock(&master_lock);
    for (ns=results, i=0; ns != NULL; ns=ns->dh.next, i++) {
        save_namespace(ns);
    }
    HASH_CLEAR(dh, results);
}

void *backup_thread(void *ctx)
{
    pthread_mutex_lock(&master_lock);
    while (is_running) {
        pthread_cond_wait(&backup_cond, &master_lock);
        pthread_mutex_unlock(&master_lock);
        save_namespaces();
    }
    return NULL;
}

void backup(int timer_fd, short event, void *arg)
{
    evtimer_set(&backup_timer, backup, NULL);
    pthread_mutex_lock(&master_lock);
    pthread_cond_signal(&backup_cond);
    pthread_mutex_unlock(&master_lock);
    evtimer_add(&backup_timer, &backup_tv);
}

void put_cb(struct evhttp_request *req, void *arg)
{
    struct evbuffer *buf = evbuffer_new();
    struct evkeyvalq args;
    struct el *e;
    char *namespace, *key, *id, *data, *ts, *locale;
    time_t when = time(NULL);

    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    key =       (char *)evhttp_find_header(&args, "key");
    data =      (char *)evhttp_find_header(&args, "data");
    id =        (char *)evhttp_find_header(&args, "id");
    locale =    (char *)evhttp_find_header(&args, "locale");
    ts =        (char *)evhttp_find_header(&args, "ts");
    if (ts) {
        when = (time_t)strtol(ts, NULL, 10);
    }

    if (namespace && key) {
        e = put_el(namespace, locale, key, id, data, when, 1);
        if (e) {
            e->count += 1;
            evhttp_send_reply(req, HTTP_OK, "OK", buf);
        } else {
            evhttp_send_reply(req, HTTP_INTERNAL, "ERR", buf);
        }
    } else {
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_REQ_ARG", buf);
    }

    evhttp_clear_headers(&args);
    evbuffer_free(buf);
}

void del_cb(struct evhttp_request *req, void *arg)
{
    struct evbuffer *buf = evbuffer_new();
    struct evkeyvalq args;
    struct namespace *ns;
    composite_key *ckey;
    struct el *e;
    char *namespace, *key, *id, *locale;
    
    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    key =       (char *)evhttp_find_header(&args, "key");
    id =        (char *)evhttp_find_header(&args, "id");
    locale =    (char *)evhttp_find_header(&args, "locale");
    
    if (namespace && key) {
        ns = get_namespace(namespace);
        if (ns) {
            ckey = make_key(locale, key, id);
            pthread_mutex_lock(&ns->lock);            
            HASH_FIND(hh, ns->elems, ckey->data, KEY_LEN(ckey), e);
            if (e) {
                HASH_DEL(ns->elems, e);
            }
            pthread_mutex_unlock(&ns->lock);
            free_el(e);
            safe_free(ckey);
        }        
        evhttp_send_reply(req, HTTP_OK, "OK", buf);
    } else {
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_REQ_ARG", buf);
    }
    
    evhttp_clear_headers(&args);
    evbuffer_free(buf);
}

void nuke_cb(struct evhttp_request *req, void *arg)
{
    struct evbuffer *buf = evbuffer_new();
    struct evkeyvalq args;
    struct namespace *ns;
    composite_key *ckey;
    char *namespace, *key, *id, *locale;
    struct el *e, *results = NULL, *tmp = NULL;
    
    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    key =       (char *)evhttp_find_header(&args, "key");
    id =        (char *)evhttp_find_header(&args, "id");
    locale =    (char *)evhttp_find_header(&args, "locale");
    
    if (namespace) {
        ns = get_namespace(namespace);
        if (ns) {
            ckey = make_key(locale, key, id);
            pthread_mutex_unlock(&ns->lock);
            if (id) {
                HASH_SELECT(rh, results, hh, ns->elems, key_id_match);
            } else {
                HASH_SELECT(rh, results, hh, ns->elems, key_match);
            }
            HASH_ITER(rh, results, e, tmp) {
                HASH_DEL(ns->elems, e);  /* delete; users advances to next */
                free_el(e);
            }
            safe_free(ckey);
            HASH_CLEAR(rh, results);
            pthread_mutex_unlock(&ns->lock);
        }        
        evhttp_send_reply(req, HTTP_OK, "OK", buf);
    } else {
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_REQ_ARG", buf);
    }
    
    evhttp_clear_headers(&args);
    evbuffer_free(buf);
}

void search_cb(struct evhttp_request *req, void *arg)
{
    struct evbuffer *buf = evbuffer_new();
    struct evkeyvalq args;
    composite_key *ckey;
    char *namespace, *slimit, *locale, *ts, *id, *key;
    struct json_object *jsobj, *jsel, *jsresults;
    struct el *e, *results = NULL;
    struct namespace *ns;
    int i, new, limit = 100;
    time_t when = 0;
    
    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    key =       (char *)evhttp_find_header(&args, "key");
    id =        (char *)evhttp_find_header(&args, "id");
    locale =    (char *)evhttp_find_header(&args, "locale");
    slimit =    (char *)evhttp_find_header(&args, "limit");
    ts =        (char *)evhttp_find_header(&args, "ts");
    if (slimit) {
        limit = atoi(slimit);
    }
    if (ts) {
        when = (time_t)strtol(ts, NULL, 10);
    }
    
    if (namespace) {
        jsobj = json_object_new_object();
        jsresults = json_object_new_array();
        ns = create_namespace(namespace, &new);
        if (ns) {
            ckey = make_key(locale, key, id);
            pthread_mutex_lock(&ns->lock);            
            
            if (id) {
                HASH_SELECT(rh, results, hh, ns->elems, key_id_match);
            } else {
                HASH_SELECT(rh, results, hh, ns->elems, key_match);
            }
            HASH_SRT(rh, results, time_count_sort);
            for (e=results, i=0; e != NULL && i < limit && e->when > when; e=e->rh.next, i++) {
                jsel = json_object_new_object();
                json_object_object_add(jsel, "key", json_object_new_string(e->ckey->key));
                json_object_object_add(jsel, "id", json_object_new_string(e->ckey->id));
                json_object_object_add(jsel, "when", json_object_new_int(e->when));
                json_object_object_add(jsel, "count", json_object_new_int(e->count));
                if (e->data != NULL) {
                    json_object_object_add(jsel, "data", json_object_new_string(e->data));
                }
                json_object_array_add(jsresults, jsel);
            }
            HASH_CLEAR(rh, results);

            pthread_mutex_unlock(&ns->lock);
            safe_free(ckey);
        }
        json_object_object_add(jsobj, "results", jsresults);
        evbuffer_add_printf(buf, "%s\n", (char *)json_object_to_json_string(jsobj));
        evhttp_send_reply(req, HTTP_OK, "OK", buf);
        json_object_put(jsobj);
    } else {
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_REQ_ARG", buf);
    }
    
    evhttp_clear_headers(&args);
    evbuffer_free(buf);
}


void termination_handler(int signum)
{
    fprintf(stdout, "Shutting down...\n");
    event_loopbreak();
    is_running = 0;
}

int main(int argc, char **argv)
{
    int opt;
    int port = DEFAULT_PORT;
    pthread_t id;
    char *address = "0.0.0.0";
    UErrorCode err = U_ZERO_ERROR;

    while((opt = getopt(argc, argv, "a:d:p:l:")) != -1) {
        switch(opt) {
            case 'a':
                address = optarg;
                break;
            case 'd':
                db_dir = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'l':
                default_locale = optarg;
                break;
            case '?':
                fprintf (stderr, "Unknown option: '-%c'\n", optopt);
                return 1;
        }
    }

    signal(SIGINT, termination_handler);
    signal(SIGQUIT, termination_handler);
    signal(SIGTERM, termination_handler);
    signal(SIGPIPE, SIG_IGN);

    pthread_mutex_init(&master_lock, NULL);
    pthread_cond_init(&backup_cond, NULL);
    uloc_setDefault(default_locale, &err);
    if (U_FAILURE(err)) {
        fprintf(stderr, "Could not set default location: %s: %s\n", default_locale, u_errorName(err));
        exit(1);
    }

    if (db_dir) {
        if (db_dir[strlen(db_dir)] == '/') {
            db_dir[strlen(db_dir)] = '\0';
        }
        make_nested_dirs();
    }
    event_init();
    pthread_create(&id, NULL, backup_thread, NULL);
    pthread_detach(id);
    backup(0,0,NULL);

    httpd = evhttp_start(address, port);
    if (httpd == NULL) {
        fprintf(stdout, "Could not listen on: %s:%d\n", address, port);
        return 1;
    }

    evhttp_set_cb(httpd, "/put", put_cb, NULL);
    evhttp_set_cb(httpd, "/del", del_cb, NULL);
    evhttp_set_cb(httpd, "/nuke", nuke_cb, NULL);
    evhttp_set_cb(httpd, "/search", search_cb, NULL);
    fprintf(stdout, "Starting %s (%s) listening on: %s:%d\n", NAME, VERSION, address, port);

    event_dispatch();
    evhttp_free(httpd);
    save_namespaces();
    return 0;
}
