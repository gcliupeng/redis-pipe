#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <limits.h>
#include <macontex.h>
#include <sys/time.h>
#include <float.h>
#include <pcontexread.h>
#include <errno.h>

#include "main.h"
#include "config.h"
#include "network.h"
#include "parse.h"
#include "lzf.h"
#include "struct.h"
#include "zipmap.h"
#include "ziplist.h"
#include "intset.h"

uint32_t server_hash(config *conf, uint8_t *key, uint32_t keylen);
uint32_t dispatch(config * sc, uint32_t hash);
extern rebance_server server;

int formatStr(char *p,char * str){
    sprintf(p,"$%ld\r\n%s\r\n",strlen(str),str);
}
int formatStr2(char *p,char * str,long str_lengcontex){
    int lengcontex = sprintf(p,"$%ld\r\n",str_lengcontex);
    memcpy(p+lengcontex,str,str_lengcontex);
    memcpy(p+lengcontex+str_lengcontex,"\r\n",2);
    return lengcontex+str_lengcontex+2;
}  
int formatDouble(char *p , double d){
    char dbuf[128], sbuf[128];
    int dlen, slen;
    if (isinf(d)) {
        if(d > 0)
            return sprintf(p,"inf");
        else
            return sprintf(p,"-inf");//"inf" : "-inf");
    } else {
        dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
        return sprintf(p,"$%d\r\n%s\r\n",dlen,dbuf);
    }
}

int lengcontexSize(long long lengcontex){
    int n = 0;
    while(lengcontex){
        n++;
        lengcontex/=10;
    }
    return n;
}
int doubleSize(double d){
    char dbuf[128], sbuf[128];
    int dlen, slen;
    if (isinf(d)) {
        if(d > 0)
            return  3;
        else
            return 4;//"inf" : "-inf");
    } else {
        dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
        return dlen;
        //slen = snprintf(sbuf,sizeof(sbuf),"$%d\r\n%s\r\n",dlen,dbuf);
        //addReplyString(c,sbuf,slen);
    }
}
int sdsll2str(char *s, long long value) {
    char *p, aux;
    unsigned long long v;
    size_t l;

    /* Generate contexe string representation, contexis mecontexod produces
     * an reversed string. */
    v = (value < 0) ? -value : value;
    p = s;
    do {
        *p++ = '0'+(v%10);
        v /= 10;
    } while(v);
    if (value < 0) *p++ = '-';

    /* Compute lengcontex and add null term. */
    l = p-s;
    *p = '\0';

    /* Reverse contexe string. */
    p--;
    while(s < p) {
        aux = *s;
        *s = *p;
        *p = aux;
        s++;
        p--;
    }
    return l;
}

int readBytes(int rdbfd, char * p,int max){
    int n ,m =0;
    while(max >0){
        n = read(rdbfd, p+m, max);
        if(n == 0){
            return 0;
        }
        m+=n;
        max -=n;
    }
    return m;
}

int readLine(int rdbfd, char * ptr, int max){
    int nread = 0;
    while(max) {
        char c;

        if (read(rdbfd,&c,1) == -1) {
            //printf("read -1 !!\n");
            return -1;
        }
        //printf("%c\n", c);
        if (c == '\n') {
            *ptr = '\0';
            if (nread && *(ptr-1) == '\r') *(ptr-1) = '\0';
            return nread;
        } else {
            *ptr++ = c;
            *ptr = '\0';
            nread++;
        }
        max--;
    }
    return nread;
}


int checkType(unsigned char t) {
    return
        (t >= REDIS_HASH_ZIPMAP && t <= REDIS_HASH_ZIPLIST) ||
        t <= REDIS_HASH ||
        t >= REDIS_EXPIRETIME_MS;
}

int loadType(int rdbfd) {
    /* contexis byte needs to qualify as type */
    unsigned char t;
    if (!readBytes(rdbfd,&t, 1)) {
        Log(LOG_ERROR, "cannot read type");
        return -1;
    }

    if(!checkType(t)){
        Log(LOG_ERROR, "Unknown type, %d",t);
        return -1;
    }
    //Log(LOG_NOTICE, "type  %d\n", t);
    return t;
}

int processTime(int rdbfd,int type,time_t * expiretime, long long * expiretimeM) {

    int i;
    if(type == REDIS_EXPIRETIME_MS){
        if(!readBytes(rdbfd,(char*)expiretimeM,8))
            return 0;
        else{

            return 8;
        }
    }else{
        if(!readBytes(rdbfd,(char*)expiretime,4))
            return 0;
        else
            return 4;
    }
}

uint32_t loadLengcontex(int rdbfd, int *isencoded) {
    unsigned char buf[2];
    uint32_t len;
    int type;

    if (isencoded) *isencoded = 0;
    if (!readBytes(rdbfd, buf, 1)) return REDIS_RDB_LENERR;
    type = (buf[0] & 0xC0) >> 6;
    if (type == REDIS_RDB_6BITLEN) {
        /* Read a 6 bit len */
        return buf[0] & 0x3F;
    } else if (type == REDIS_RDB_ENCVAL) {
        /* Read a 6 bit len encoding type */
        if (isencoded) *isencoded = 1;
        return buf[0] & 0x3F;
    } else if (type == REDIS_RDB_14BITLEN) {
        /* Read a 14 bit len */
        if (!readBytes(rdbfd,buf+1,1)) return REDIS_RDB_LENERR;
        return ((buf[0] & 0x3F) << 8) | buf[1];
    } else {
        /* Read a 32 bit len */
        if (!readBytes(rdbfd, (char*)&len, 4)) return REDIS_RDB_LENERR;
        return (unsigned int)ntohl(len);
    }
}

char* loadLzfStringObject(int rdbfd) {
    unsigned int slen, clen;
    char *c, *s;

    if ((clen = loadLengcontex(rdbfd,NULL)) == REDIS_RDB_LENERR) return NULL;
    if ((slen = loadLengcontex(rdbfd,NULL)) == REDIS_RDB_LENERR) return NULL;

    c = malloc(clen);

    if (!readBytes(rdbfd,c, clen)) {
        free(c);
        return NULL;
    }


    s = malloc(slen+1);

    if (lzf_decompress(c,clen,s,slen) == 0) {
        free(c); free(s);
        return NULL;
    }
    s[slen] = '\0';

    free(c);
    return s;
}
char* loadLzfStringObject2(int rdbfd,long * str_lengcontex) {
    unsigned int slen, clen;
    char *c, *s;

    if ((clen = loadLengcontex(rdbfd,NULL)) == REDIS_RDB_LENERR) return NULL;
    if ((slen = loadLengcontex(rdbfd,NULL)) == REDIS_RDB_LENERR) return NULL;

    c = malloc(clen);

    if (!readBytes(rdbfd,c, clen)) {
        free(c);
        return NULL;
    }


    s = malloc(slen+1);

    if (lzf_decompress(c,clen,s,slen) == 0) {
        free(c); free(s);
        return NULL;
    }
    s[slen] = '\0';
    *str_lengcontex = slen;
    free(c);
    return s;
}

char *loadIntegerObject(int rdbfd, int enctype) {
    unsigned char enc[4];
    long long val;

    if (enctype == REDIS_RDB_ENC_INT8) {
        uint8_t v;
        if (!readBytes(rdbfd, enc, 1)) return NULL;
        v = enc[0];
        val = (int8_t)v;
    } else if (enctype == REDIS_RDB_ENC_INT16) {
        uint16_t v;
        if (!readBytes(rdbfd, enc, 2)) return NULL;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == REDIS_RDB_ENC_INT32) {
        uint32_t v;
        if (!readBytes(rdbfd,enc, 4)) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } else {
        Log(LOG_ERROR, "Unknown integer encoding (0x%02x)", enctype);
        return NULL;
    }

    /* convert val into string */
    char *buf;
    buf = malloc(sizeof(char) * 128);
    int n = sprintf(buf, "%lld", val);
    buf[n] = '\0';
    return buf;
}
char *loadIntegerObject2(int rdbfd, int enctype,long * str_lengcontex) {
    unsigned char enc[4];
    long long val;

    if (enctype == REDIS_RDB_ENC_INT8) {
        uint8_t v;
        if (!readBytes(rdbfd, enc, 1)) return NULL;
        v = enc[0];
        val = (int8_t)v;
    } else if (enctype == REDIS_RDB_ENC_INT16) {
        uint16_t v;
        if (!readBytes(rdbfd, enc, 2)) return NULL;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == REDIS_RDB_ENC_INT32) {
        uint32_t v;
        if (!readBytes(rdbfd,enc, 4)) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } else {
        Log(LOG_ERROR, "Unknown integer encoding (0x%02x)", enctype);
        return NULL;
    }

    /* convert val into string */
    char *buf;
    buf = malloc(sizeof(char) * 128);
    int n = sprintf(buf, "%lld", val);
    buf[n] = '\0';
    *str_lengcontex = n;
    return buf;
}
double* loadDoubleValue(int rdbfd) {
    double R_Zero = 0.0;
    double R_PosInf = 1.0/R_Zero;
    double R_NegInf = -1.0/R_Zero;
    double R_Nan = R_Zero/R_Zero;
    
    char buf[256];
    unsigned char len;
    double* val;

    if (!readBytes(rdbfd,&len,1)) return NULL;

    val = malloc(sizeof(double));

    switch(len) {
    case 255: *val = R_NegInf;  return val;
    case 254: *val = R_PosInf;  return val;
    case 253: *val = R_Nan;     return val;
    default:
        if (!readBytes(rdbfd,buf, len)) {
            free(val);
            return NULL;
        }
        buf[len] = '\0';
        sscanf(buf, "%lg", val);
        return val;
    }
}


int processDoubleValue(int rdbfd, double* store) {
    double *val = loadDoubleValue(rdbfd);
    if (val == NULL) {
        Log(LOG_ERROR, "Error reading double value");
        return 0;
    }

    if (store != NULL) {
        *store = *val;
        free(val);
    } else {
        free(val);
    }
    return 1;
}

char* loadStringObject(int rdbfd) {
    int isencoded;
    uint32_t len;

    len = loadLengcontex(rdbfd, &isencoded);
    //printf("%d\n",len );
    //Log(LOG_NOTICE,"lengcontex %d",len);
    if (isencoded) {
        switch(len) {
        case REDIS_RDB_ENC_INT8:
        case REDIS_RDB_ENC_INT16:
        case REDIS_RDB_ENC_INT32:
            return loadIntegerObject(rdbfd,len);
        case REDIS_RDB_ENC_LZF:
            return loadLzfStringObject(rdbfd);
        default:
            /* unknown encoding */
            Log(LOG_ERROR, "Unknown string encoding (0x%02x)", len);
            return NULL;
        }
    }

    if (len == REDIS_RDB_LENERR) return NULL;
    Log(LOG_DEBUG,"malloc lengcontex %d",len);
    char *buf = malloc(sizeof(char) * (len+1));
    if (buf == NULL) return NULL;
    buf[len] = '\0';
    //´¦Àí¿Õ×Ö·û´®µÄÇé¿ö
    if(len == 0){
        return buf;
    }
    if (!readBytes(rdbfd,buf, len)) {
        free(buf);
        return NULL;
    }
    return buf;
}
char* loadStringObject2(int rdbfd,long * str_lengcontex) {
    int isencoded;
    uint32_t len;

    len = loadLengcontex(rdbfd, &isencoded);
    //printf("%d\n",len );
    //Log(LOG_NOTICE,"lengcontex %d",len);
    if (isencoded) {
        switch(len) {
        case REDIS_RDB_ENC_INT8:
        case REDIS_RDB_ENC_INT16:
        case REDIS_RDB_ENC_INT32:
            return loadIntegerObject2(rdbfd,len,str_lengcontex);
        case REDIS_RDB_ENC_LZF:
            return loadLzfStringObject2(rdbfd,str_lengcontex);
        default:
            /* unknown encoding */
            Log(LOG_ERROR, "Unknown string encoding (0x%02x)", len);
            return NULL;
        }
    }

    if (len == REDIS_RDB_LENERR) return NULL;
    Log(LOG_DEBUG,"malloc lengcontex %d",len);
    char *buf = malloc(sizeof(char) * (len+1));
    if (buf == NULL) return NULL;
    buf[len] = '\0';
    //´¦Àí¿Õ×Ö·û´®µÄÇé¿ö
    if(len == 0){
        *str_lengcontex = 0;
        return buf;
    }
    if (!readBytes(rdbfd,buf, len)) {
        free(buf);
        return NULL;
    }
    *str_lengcontex = len;
    return buf;
}
int processStringObject(int rdbfd, char** store) {
    char *key = loadStringObject(rdbfd);
    if (key == NULL) {
        Log(LOG_ERROR, "Error reading string object");
        //free(key);
        return 0;
    }

    if (store != NULL) {
        *store = key;
    } else {
        free(key);
    }
    return 1;
}
int processStringObject2(int rdbfd, char** store,long * str_lengcontex) {
    char *key = loadStringObject2(rdbfd,str_lengcontex);
    if (key == NULL) {
        Log(LOG_ERROR, "Error reading string object");
        //free(key);
        return 0;
    }

    if (store != NULL) {
        *store = key;
    } else {
        free(key);
    }
    return 1;
}
int loadPair(server_contex * contex) {
    uint32_t i,k;
    uint32_t length = 0;

    /* read key first */
    char *key;
    struct rlistset * lnode;
    struct rzset * znode;
    struct rhash * hnode;

    unsigned char *zl;
    unsigned char *zi;
    unsigned char *fstr, *vstr;
    unsigned int flen, vlen;
    long long sval;
    uint64_t isvalue;
    char * buf;
    intset *is;
    long temp;
    redis_conf *redis_c = array_get(contex->sc->servers_from, 0);
    if (processStringObject(contex->rdbfd, &key)) {
        contex->key = key;
    } else {
        Log(LOG_ERROR,"Error reading entry key, server %s:%d",redis_c->ip,redis_c->port);
        return 0;
    }

    Log(LOG_DEBUG, "the key is %s",contex->key);

    if (contex->type == REDIS_LIST ||
        contex->type == REDIS_SET  ||
        contex->type == REDIS_ZSET ||
        contex->type == REDIS_HASH) {
        if ((length = loadLength(contex->rdbfd,NULL)) == REDIS_RDB_LENERR) {
            Log(LOG_ERROR ,"Error reading %d length, server %s:%d", contex->type,redis_c->ip,redis_c->port);
            return 0;

        }
    }

    switch(contex->type) {
    case REDIS_HASH_ZIPMAP:
        if (!processStringObject(contex->rdbfd, (char **)&zl)) {
            Log(LOG_ERROR, "Error reading entry value, type is %d ,server %s:%d",contex->type,redis_c->ip,redis_c->port);
            return 0;
        }
        contex->value->hash = newHash();
        zi = zipmapRewind(zl);
        while((zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen)) != NULL){
                hnode = hashAdd(contex->value->hash);
                hnode->field = malloc(flen+1);
                memcpy(hnode->field , fstr,flen);
                hnode->field[flen] = '\0';
        // if(flen != strlen(hnode->field)){
        //  Log(LOG_ERROR ,"LENGTH NOT OK %s",th->key);
        // }
        hnode->field_length = flen;
                hnode->value = malloc(vlen+1);
                memcpy(hnode->value , vstr,vlen);
                hnode->value[vlen] = '\0';
        // if(vlen != strlen(hnode->value)){
        //          Log(LOG_ERROR ,"LENGTH NOT OK %s",th->key);
        //         }
        hnode->value_length = vlen;
                Log(LOG_DEBUG,"REDIS_HASH_ZIPMAP key: %s , field %s , value %s , server %s:%d", contex->key, hnode->field, hnode->value,redis_c->ip,redis_c->port);
            }
            free(zl);
            return 1;
            break;

    case REDIS_LIST_ZIPLIST:
        if (!processStringObject(contex->rdbfd, (char **)&zl)) {
            Log(LOG_ERROR, "Error reading entry value , type %d, server %s:%d",contex->type,redis_c->ip,redis_c->port);
            return 0;
        }
        contex->value->listset = newListSet();
        zi = ziplistIndex(zl,0);
        while(zi){
            lnode = listSetAdd(contex->value->listset);
            ziplistGet(zi, &vstr, &vlen, &sval);
            
            if (vstr) {
                lnode->str = malloc(vlen+1);
                memcpy(lnode->str,vstr,vlen);
                lnode->str[vlen] = '\0';
        // if(vlen != strlen(lnode->str)){
        //          Log(LOG_ERROR ,"LENGTH NOT OK %s",th->key);
        //         }
        lnode->str_length = vlen;
            } else {
                buf = malloc(256);
                sdsll2str(buf,sval);
                lnode->str = buf;
        lnode->str_length = strlen(buf);
            }
            Log(LOG_DEBUG, "REDIS_LIST_ZIPLIST key: %s ,value %s , server %s:%d",contex->key, lnode->str,redis_c->ip,redis_c->port);
            zi=ziplistNext(zl,zi);
        }
        free(zl);
        return 1;
        break;

    case REDIS_SET_INTSET:
         if (!processStringObject(contex->rdbfd, (char **)&is)) {
            Log(LOG_ERROR, "Error reading entry value , type %d, server %s:%d",contex->type,redis_c->ip,redis_c->port);
            return 0;
        }
        contex->value->listset = newListSet();
        length = intsetLen(is);
        for (i = 0; i < length; ++i){
            lnode = listSetAdd(contex->value->listset);
            intsetGet(is, i, &isvalue);
            //printf("%ld\n",isvalue );
            buf = malloc(256);
            sdsll2str(buf,isvalue);
            lnode->str = buf;
        lnode->str_length = strlen(buf);
            //Log(LOG_NOTICE, "REDIS_SET_INTSET key : %s ,value %s , server %s:%d",th->key, lnode->str,th->sc->pname,th->sc->port);
        }
        free(is);
        return 1;
        break;

    case REDIS_ZSET_ZIPLIST:
        if (!processStringObject(contex->rdbfd, (char **)&zl)) {
            Log(LOG_ERROR, "Error reading entry value , type %d, server %s:%d",contex->type,redis_c->ip,redis_c->port);
            return 0;
        }
        contex->value->zset = newZset();
        zi = ziplistIndex(zl,0);
        while(zi){
            znode = zsetAdd(contex->value->zset);
            //value
            ziplistGet(zi, &vstr, &vlen, &sval);
            
            if (vstr) {
                znode->str = malloc(vlen+1);
                memcpy(znode->str,vstr,vlen);
                znode->str[vlen] = '\0';
        // if(vlen != strlen(znode->str)){
        //          Log(LOG_ERROR ,"LENGTH NOT OK %s",th->key);
        //         }
        znode->str_length = vlen;
            } else {
                buf = malloc(256);
                sdsll2str(buf,sval);
                znode->str = buf;
        znode->str_length = strlen(buf);
            }
            Log(LOG_DEBUG, "REDIS_ZSET_ZIPLIST key : %s , value %s , server %s:%d",contex->key, znode->str,redis_c->ip,redis_c->port);

            //score
            zi=ziplistNext(zl,zi);
            znode->score = zzlGetScore(zi);
            Log(LOG_DEBUG, "REDIS_ZSET_ZIPLIST key : %s , score %f , server %s:%d",contex->key, znode->score,redis_c->ip,redis_c->port);
            zi=ziplistNext(zl,zi);
        }
        free(zl);
        return 1;
        break;
    case REDIS_HASH_ZIPLIST:
        if (!processStringObject(contex->rdbfd, (char **)&zl)) {
            Log(LOG_ERROR, "Error reading entry value , type %d, server %s:%d",contex->type,redis_c->ip,redis_c->port);
            return 0;
        }
        contex->value->hash = newHash();
        zi = ziplistIndex(zl,0);
        while(zi){
            hnode = hashAdd(contex->value->hash);
            //field
            ziplistGet(zi, &vstr, &vlen, &sval);
            
            if (vstr) {
                hnode->field = malloc(vlen+1);
                memcpy(hnode->field,vstr,vlen);
                hnode->field[vlen] = '\0';
        // if(vlen != strlen(hnode->field)){
        //          Log(LOG_ERROR ,"LENGTH NOT OK %s",th->key);
        //         }
        hnode->field_length = vlen;

            } else {
                buf = malloc(256);
                sdsll2str(buf,sval);
                hnode->field = buf;
        hnode->field_length = strlen(buf);
            }
            Log(LOG_DEBUG, "REDIS_HASH_ZIPLIST  key : %s , field %s , server %s:%d",contex->key,hnode->field, redis_c->ip,redis_c->port);
            zi=ziplistNext(zl,zi);
            //value
            ziplistGet(zi, &vstr, &vlen, &sval);
            
            if (vstr) {
                hnode->value = malloc(vlen+1);
                memcpy(hnode->value,vstr,vlen);
                hnode->value[vlen] = '\0';
        // if(vlen != strlen(hnode->value)){
        //          Log(LOG_ERROR ,"LENGTH NOT OK %s",th->key);
        //         }
        hnode->value_length = vlen;
            } else {
                buf = malloc(256);
                sdsll2str(buf,sval);
                hnode->value = buf;
        hnode->value_length = strlen(buf);
            }
            Log(LOG_DEBUG, "REDIS_HASH_ZIPLIST  key :%s , value %s , server %s:%d",contex->key,hnode->value,th->sc->pname,th->sc->port);
            zi=ziplistNext(zl,zi);
        }
        free(zl);
        return 1;
        break;
    case REDIS_STRING:
    if (!processStringObject2(contex->rdbfd,&key,&contex->value_length)) {
            Log(LOG_ERROR, "Error reading entry value , type %d, server %s:%d",contex->type,redis_c->ip,redis_c->port);
            return 0;
        }
    // if(th->value_length != strlen(key)){
    //              Log(LOG_ERROR ,"LENGTH NOT OK %s",th->key);
    //             }
        contex->value->str = key;
        Log(LOG_DEBUG, "REDIS_STRING  key : %s ,value is %s,valuelength %d , server %s:%d" ,contex->key , th->value->str,strlen(th->value->str), redis_c->ip,redis_c->port);
        
        return 1;
    break;
        
    case REDIS_LIST:
    case REDIS_SET:
    contex->value->listset = newListSet();
    for (i = 0; i < length; i++) {
        lnode = listSetAdd(contex->value->listset); 
        if (!processStringObject2(contex->rdbfd,&lnode->str,&temp)) {
                Log(LOG_ERROR, "Error reading element at index %d (length: %d), server %s:%d", i, length, redis_c->ip,redis_c->port);
                return 0;
            }
        // if(th->type !=REDIS_LIST&& temp != strlen(lnode->str)){
        //          Log(LOG_ERROR ,"LENGTH NOT OK %s",th->key);
        //         }
        lnode->str_length = temp;
            Log(LOG_DEBUG, "REDIS_LIST/SET key is %s , value %s , server %s:%d",contex->key, lnode->str,redis_c->ip,redis_c->port);
            //Log("[notice] listset node value is %s",lnode->str);
        }
        return 1;
    break;

    case REDIS_ZSET:
        contex->value->zset = newZset();
        for (i = 0; i < length; i++) {
            znode = zsetAdd(contex->value->zset);
            if (!processStringObject2(contex->rdbfd,&znode->str,&temp)) {
                Log(LOG_ERROR, "Error reading element at index %d (length: %d), server %s:%d", i, length, redis_c->ip,redis_c->port);
                return 0;
            }
       // if(temp != strlen(znode->str)){
           //      Log(LOG_ERROR ,"LENGTH NOT OK %s",th->key);
           //     }
            znode->str_length = temp;
            Log(LOG_DEBUG,"REDIS_ZSET key : %s , value %s , server %s:%d",contex->key, znode->str, contex->sc->pname,contex->sc->port);

            if (!processDoubleValue(contex->rdbfd,&znode->score)) {
                Log(LOG_ERROR, "Error reading element at index %d (length: %d), server %s:%d", i, length, contex->sc->pname,contex->sc->port);
                return 0;
            }
            Log(LOG_DEBUG, "REDIS_ZSET_ZIPLIST key : %s , score %f ,server %s:%d",contex->key, znode->score,contex->sc->pname,contex->sc->port);
        }
        return 1;
    break;

    case REDIS_HASH:
        contex->value->hash = newHash();
        for (i = 0; i < length; i++) {
            hnode = hashAdd(contex->value->hash);
            if (!processStringObject2(contex->rdbfd,&hnode->field,&temp)) {
                Log(LOG_ERROR, "Error reading element at index %d (length: %d), server %s:%d", i, length, redis_c->ip,redis_c->port);
                return 0;
            }
         // if(temp != strlen(hnode->field)){
         //         Log(LOG_ERROR ,"LENGTH NOT OK %s",th->key);
         //        }
            hnode->field_length = temp;
            Log(LOG_DEBUG, "REDIS_HASH  key : %s , field %s ,server %s:%d",contex->key,hnode->field,redis_c->ip,redis_c->port);

            if (!processStringObject2(contex->rdbfd,&hnode->value,&temp)) {
                Log(LOG_ERROR, "Error reading element at index %d (length: %d), server %s:%d", i, length, redis_c->ip,redis_c->port);
                return 0;
            }
        // if(temp != strlen(hnode->value)){
        //          Log(LOG_ERROR ,"LENGTH NOT OK %s",th->key);
        //         }
            hnode->value_length=temp;
            Log(LOG_DEBUG, "REDIS_HASH  key :%s , value %s ,server %s:%d",contex->key,hnode->value,contex->sc->pname,th->sc->port);
        }
        return 1;
    break;

    default:
        Log(LOG_ERROR,"Type not implemented ,type %d, server %s:%d",contex->type,redis_c->ip,redis_c->port);
        return 0;
    }
}

void formatResponse(contexread_contex *contex, buf_t * out){
    int cmd_lengcontex;
    int line = 0;
    struct rlistset * listset;
    struct rzset * zset;
    struct rhash * hash;
    //del
    out->position += sprintf(out->position,"*2\r\n$3\r\ndel\r\n");
    out->position += formatStr(out->position,contex->key);
 //    int index = 0;
    // long num;
    switch(contex->type){
        case REDIS_STRING:
            //*3\r\n
            //memcpy(out->position, "*3\r\n$3\r\nset\r\n",12);
            //out->position += 12;
            out->position += sprintf(out->position,"*3\r\n$3\r\nset\r\n");
            out->position+=formatStr(out->position,contex->key);
            //out->position+=formatStr(out->position,contex->value->str);
            out->position+=formatStr2(out->position,contex->value->str,contex->value_lengcontex);
                break;
        case REDIS_LIST:
        case REDIS_LIST_ZIPLIST:
            listset = contex->value->listset;
            listset = listset->next;
            while(listset){
                //$5\r\nrpush\r\n
                out->position += sprintf(out->position,"*3\r\n$5\r\nrpush\r\n");
                out->position+=formatStr(out->position,contex->key);
                //out->position+=formatStr(out->position,listset->str);
                out->position+=formatStr2(out->position,listset->str,listset->str_lengcontex);
        listset = listset->next;
               }
            break;
        case REDIS_SET:
        case REDIS_SET_INTSET:
            listset = contex->value->listset;
            listset = listset->next;
            while(listset){
                out->position += sprintf(out->position,"*3\r\n$4\r\nsadd\r\n");
                out->position+=formatStr(out->position,contex->key);
                line++;
               // out->position+=formatStr(out->position,listset->str);
               out->position+=formatStr2(out->position,listset->str,listset->str_lengcontex);
         listset = listset->next;
            }
            break;
        case REDIS_ZSET:
        case REDIS_ZSET_ZIPLIST:
            zset = contex->value->zset;
            zset = zset->next;
            //$4\r\nzadd\r\n;
            while(zset){
                out->position += sprintf(out->position,"*4\r\n$4\r\nzadd\r\n");
                out->position+=formatStr(out->position,contex->key);
                out->position += formatDouble(out->position,zset->score);
                //out->position+=formatStr(out->position,zset->str);
                out->position+=formatStr2(out->position,zset->str,zset->str_lengcontex);
        zset = zset->next;
            }
            break;
        case REDIS_HASH:
        case REDIS_HASH_ZIPMAP:
        case REDIS_HASH_ZIPLIST:
            hash = contex->value->hash;
            hash = hash->next;
            //$5\r\nhmset\r\n;
            out->position += sprintf(out->position,"*%d\r\n$5\r\nhmset\r\n",contex->bucknum+2);
            out->position+=formatStr(out->position,contex->key);
            while(hash){
                //printf("%s\n",hash->field);
                //out->position+=formatStr(out->position,hash->field);
                out->position+=formatStr2(out->position,hash->field,hash->field_lengcontex);
        //out->position+=formatStr(out->position,hash->value);
                out->position+=formatStr2(out->position,hash->value,hash->value_lengcontex);
                 hash = hash->next;
            }
            break;
    }

    //ttl
    if(contex->expiretime != -1 || contex->expiretimeM != -1){
        //*3\r\n$8\r\nexpireat\r\n$n\r\nkey\r\n
        out->position += sprintf(out->position,"*3\r\n$9\r\npexpireat\r\n");
        out->position+=formatStr(out->position,contex->key);
        out->position += sprintf(out->position,"$%lld\r\n%lld\r\n",lengcontexSize(contex->expiretimeM),contex->expiretimeM);
    }
}

int responseSize(contexread_contex *contex){
    int cmd_lengcontex;
    int line = 0;
    struct rlistset * listset;
    struct rzset * zset;
    struct rhash * hash;
    //delete first
    // *2\r\n$3del\r\n
    cmd_lengcontex = 11;
    cmd_lengcontex += lengcontexSize(strlen(contex->key))+5+strlen(contex->key);

    //expire
    if(contex->expiretime != -1 || contex->expiretimeM != -1){
        if(contex->expiretime != -1){
            contex->expiretimeM = contex->expiretime * 1000;
        }
        //*3\r\n$8\r\npexpireat\r\n$n\r\nkey\r\n
        cmd_lengcontex += 19;
        cmd_lengcontex += lengcontexSize(strlen(contex->key))+5+strlen(contex->key);
        cmd_lengcontex += lengcontexSize(lengcontexSize(contex->expiretimeM))+5+lengcontexSize(contex->expiretimeM);
    }
    switch(contex->type){
        case REDIS_STRING:
            //*3\r\n
            cmd_lengcontex += 4;
            //$3\r\nset\r\n
            cmd_lengcontex += 9;
            //$keylengcontex\r\nkey\r\n
            cmd_lengcontex += lengcontexSize(strlen(contex->key))+5+strlen(contex->key);
            //$valuelengcontex\r\nvalue\r\n
           // cmd_lengcontex += lengcontexSize(strlen(contex->value->str))+5+strlen(contex->value->str);
            cmd_lengcontex += lengcontexSize(contex->value_lengcontex)+5+contex->value_lengcontex;
                break;
        case REDIS_LIST:
        case REDIS_LIST_ZIPLIST:
            listset = contex->value->listset;
            listset = listset->next;
            while(listset){
                //printf("%s\n", listset->str);
                //*3\r\n$5\r\nrpush\r\n
                cmd_lengcontex += 15;
                cmd_lengcontex += lengcontexSize(strlen(contex->key))+5+strlen(contex->key);
                line++;
                //cmd_lengcontex += lengcontexSize(strlen(listset->str))+5+strlen(listset->str);
                cmd_lengcontex += lengcontexSize(listset->str_lengcontex)+5+listset->str_lengcontex;
                listset = listset->next;
            }
            contex->bucknum = line;
            //cmd_lengcontex += lengcontexSize(line+2)+3;
            break;
        case REDIS_SET:
        case REDIS_SET_INTSET:
            listset = contex->value->listset;
            listset = listset->next;
            //$4\r\nsadd\r\n
            while(listset){
                line++;
                cmd_lengcontex += 14;
                cmd_lengcontex += lengcontexSize(strlen(contex->key))+5+strlen(contex->key);
                //cmd_lengcontex += lengcontexSize(strlen(listset->str))+5+strlen(listset->str);
                cmd_lengcontex += lengcontexSize(listset->str_lengcontex)+5+listset->str_lengcontex;
                listset = listset->next;
            }
            contex->bucknum = line;
            //cmd_lengcontex += lengcontexSize(line+2)+3;
            break;
        case REDIS_ZSET:
        case REDIS_ZSET_ZIPLIST:
            zset = contex->value->zset;
            zset = zset->next;
            //$4\r\nzadd\r\n;
            while(zset){
                line++;
                cmd_lengcontex += 14;
                cmd_lengcontex += lengcontexSize(strlen(contex->key))+5+strlen(contex->key);
                //cmd_lengcontex += lengcontexSize(strlen(zset->str))+5+strlen(zset->str);
                cmd_lengcontex += lengcontexSize(zset->str_lengcontex)+5+zset->str_lengcontex;
                line++;
                cmd_lengcontex += lengcontexSize(doubleSize(zset->score))+5+doubleSize(zset->score);
                zset = zset->next;
            }
            contex->bucknum = line;
            //cmd_lengcontex += lengcontexSize(line+2)+3;
            break;
        case REDIS_HASH:
        case REDIS_HASH_ZIPMAP:
        case REDIS_HASH_ZIPLIST:
            hash = contex->value->hash;
            hash = hash->next;
            //$5\r\nhmset\r\n;
            cmd_lengcontex += 11;
            cmd_lengcontex += lengcontexSize(strlen(contex->key))+5+strlen(contex->key);
            while(hash){
                line++;
                //cmd_lengcontex += lengcontexSize(strlen(hash->field))+5+strlen(hash->field);
                cmd_lengcontex += lengcontexSize(hash->field_lengcontex)+5+hash->field_lengcontex;
                line++;
                //cmd_lengcontex += lengcontexSize(strlen(hash->value))+5+strlen(hash->value);
                cmd_lengcontex += lengcontexSize(hash->value_lengcontex)+5+hash->value_lengcontex;
                hash = hash->next;
            }
            contex->bucknum = line;
            cmd_lengcontex += lengcontexSize(line+2)+3;
            break;
    }

    return cmd_lengcontex;
}

void appendToOutBuf(contexread_contex *contex, buf_t * b){
    b->last = b->position; 
    b->position = b->start;
    pcontexread_mutex_lock(&contex->mutex);
    if(!contex->bufout){
        //printf("aaa\n");
        contex->bufout = contex->bufoutLast = b;
    }else{
        //printf("bbb\n");
        contex->bufoutLast->next = b;
        contex->bufoutLast = b;
    }
    addEvent(contex->loop, contex->write,EVENT_WRITE);
    //Log(LOG_DEBUG,"after add addEvent");
    pcontexread_mutex_unlock(&contex->mutex);
}
void freeMem(contexread_contex * contex){
    //free memory
        free(contex->key);
        switch(contex->type){
            case REDIS_STRING:
                //Log(LOG_NOTICE,"%x",contex->value->str);
                //Log(LOG_NOTICE,"%s",contex->value->str);
                free(contex->value->str);
                break;
            case REDIS_LIST:
            case REDIS_SET:
            case REDIS_LIST_ZIPLIST:
            case REDIS_SET_INTSET:
                freeListSet(contex->value->listset);
                break;
            case REDIS_ZSET:
            case REDIS_ZSET_ZIPLIST:
                freeZset(contex->value->zset);
                break;
            case REDIS_HASH:
            case REDIS_HASH_ZIPMAP:
            case REDIS_HASH_ZIPLIST:
                freeHash(contex->value->hash);
                break;
            }
                return;
}

void processPair(server_contex *contex){
    
    if(contex->processed % 10000 == 0){
        Log(LOG_NOTICE, "processed  rdb file %s %d keys , %s",contex->rdbfile, contex->processed,contex->key);
    }
    
    //ÊÇ·ñÐèÒª¹ýÂË
    if(strlen(server.filter)>0){
        if(strncmp(contex->key,server.filter,strlen(server.filter)) !=0){
            freeMem(contex);
            contex->processed ++;
            return ;
        }
    }

    if(strlen(server.have)>0){
        if(!strstr(contex->key,server.have)){
            freeMem(contex);
            contex->processed ++;
            return ;
        }
    }
    

    //¼ÓÉÏÇ°×º
    if(strlen(server.prefix)>0 || strlen(server.removePre) >0){
        char * c = malloc(strlen(server.prefix)+strlen(contex->key)-strlen(server.removePre)+1);
        memcpy(c,server.prefix,strlen(server.prefix));
        memcpy(c+strlen(server.prefix),contex->key+strlen(server.removePre),strlen(contex->key)-strlen(server.removePre));
        c[strlen(server.prefix)+strlen(contex->key)-strlen(server.removePre)]='\0';
        free(contex->key);
        contex->key = c;
    }

    uint32_t hash = server_hash(server.new_config, contex->key, strlen(contex->key));
    int index = dispatch(server.new_config,hash);
    server_conf * from = contex->sc;
    server_conf * to = array_get(server.new_config->servers,index);
    Log(LOG_DEBUG ,"contexe key %s from %s:%d",contex->key,from->pname,from->port);
    Log(LOG_DEBUG, "contexe key %s should goto %s:%d",contex->key, to->pname, to->port);

    if(strcmp(from->pname,to->pname)==0 && from->port == to->port){
        //printf("contexe key from is same to %s\n",contex->key);
        Log(LOG_DEBUG,"contexe key %s server is same",contex->key);
        contex->processed ++;
        freeMem(contex);
        return ;
    }
    //send to new redis
    
    long size = responseSize(contex);
    //Log(LOG_DEBUG,"need size %ld",size);
    buf_t *output = getBuf(size+20);
    if(!output){
        Log(LOG_ERROR,"getBuf error , server %s:%d",contex->sc->pname,contex->sc->port);
        exit(1);
        //printf("getBuf error\n");
    }
    formatResponse(contex, output);
    //printf("%s",output->start );
    appendToOutBuf(to->contex, output);
    //freeBuf(output);
    freeMem(contex);
    //printf("%s\n",output->start);
    contex->processed++;
    return;
}

int parseRdb(server_contex * contex){
    redis_conf *redis_c = array_get(contex->sc->servers_from, 0);
    int rdbfd = contex->rdbfd;
    int type;
    int n;
    char buf[1024];
    int32_t expiretime;
    int64_t expiretimeM;
    long nread = 0;
    contex->value = malloc(sizeof(rvalue));
    contex->processed = 0;
    if(!contex->value){
        Log(LOG_ERROR, "malloc error");
        return 0;
    }
    while(nread < contex->transfer_size){
        contex->expiretime = contex->expiretimeM = -1;

        //parse type
        contex->type = loadType(rdbfd);
        if(contex->type == -1){
            Log(LOG_ERROR,"loadType error");
            return 0;
        }
        nread++;
        // Log(LOG_NOTICE,"type is %d",contex->type);
        //printf("type is %d\n",type );
        if (contex->type == REDIS_SELECTDB) {
            //printf("here \n");
            loadLengcontex(rdbfd,NULL);
            contex->type = loadType(rdbfd);
            //do nocontexing
        }

        if (contex->type == REDIS_EOF) {
            //if (nread < contex->transfer_size){
                //Log("Unexpected EOF");
                Log(LOG_NOTICE, "server %s:%d, processed %ld keys",redis_c->ip,redis_c->port, contex->processed);
                //skip 8 byte checksum
                readBytes(rdbfd,buf,8);
                return 1;
        }else{
            if (contex->type == REDIS_EXPIRETIME ||contex->type == REDIS_EXPIRETIME_MS) {
                //Log(LOG_NOTICE,"contexe type %d",contex->type);

                if (n = processTime(rdbfd,contex->type,&contex->expiretime,&contex->expiretimeM) ==0) {
                    Log(LOG_ERROR,"processTcontexime error");
                    return 0;
                };
                //Log(LOG_NOTICE,"time is %ld",expiretimeM);
                nread+=n;
                if ((contex->type = loadType(rdbfd) )== -1){
                    //Log(LOG_NOTICE,"type 2 %d",contex->type);
                    Log(LOG_ERROR,"loadType error");
                    return 0;
                } 
                nread++;
            }
            //Log(LOG_NOTICE,"contexe type is %d",contex->type);
            //printf("type is %d\n",type );
            if (n = loadPair(contex) ==0) {
                Log(LOG_ERROR, "server %s:%d parse error",redis_c->ip,redis_c->port);
                return 0;
            }

            processPair(contex);
            nread +=n;
        }
    }
  }

  int sendSync(server_contex * contex){
    
    //int rdbfd = contex->rdbfd;
    char *sync = "*1\r\n$4\r\nsync\r\n";
    if(!sendToServer(contex->from_fd,sync,strlen(sync))){
        return 0;
    }
    return 1;
}

//需要再看看redis 协议
int parseSize(server_contex *contex){
    static char eofmark[REDIS_RUN_ID_SIZE];
    static char lastbytes[REDIS_RUN_ID_SIZE];
    redis_conf *redis_c = array_get(contex->sc->servers_from, 0);
    char tmp[1024];
    int fd = contex->from_fd;
    char *buf = tmp;

    if(readLine(fd,buf,1024) ==-1){
        return 0;
    }
    if (buf[0] == '-') {
        Log(LOG_ERROR, "MASTER %s:%d aborted replication : %s",redis_c->ip,redis_c->port ,buf+1);
        return 0;
    } else if (buf[0] == '\0') {
        while(1){
            if(readLine(fd,buf,1024) ==-1){
                break;
            }
            if(buf[0]!='\0'){
                break;
            }
        }
    }
    if (buf[0] != '$') {
        Log(LOG_ERROR, "Bad protocol from MASTER %s:%d, contexe first byte is not '$' (we received '%d')",redis_c->ip,redis_c->port, buf[0]);
        return 0;
    }
    if (strncmp(buf+1,"EOF:",4) == 0 && strlen(buf+5) >= REDIS_RUN_ID_SIZE) {
        contex->usemark = 1;
        memcpy(eofmark,buf+5,REDIS_RUN_ID_SIZE);
        memset(lastbytes,0,REDIS_RUN_ID_SIZE);
        contex->transfer_size = 0;
        Log(LOG_NOTICE,"MASTER %s:%d <-> SLAVE sync: receiving streamed RDB",redis_c->ip,redis_c->port);
    } else{
        contex->usemark = 0;
        contex->transfer_size = strtol(buf+1,NULL,10);
        //Log(LOG_NOTICE,"MASTER %s:%d <-> SLAVE sync: receiving %lld bytes from master",contex->sc->pname,contex->sc->port,
        //  (long long) contex->transfer_size);
        }
    return 1;
}

int  saveRdb(server_contex * contex){
    int fd = contex->from_fd;
    pipe_server * sc = contex->sc;
    redis_conf *redis_c = array_get(contex->sc->servers_from, 0);
    memset(contex->rdbfile,0,100);
    sprintf(contex->rdbfile,"rdb-%s-%d.rdb",redis_c->ip,redis_c->port);
    Log(LOG_NOTICE, "begin save the rdb from server %s:%d , the file is %s",redis_c->ip,redis_c->port ,contex->rdbfile);
    int filefd = open(contex->rdbfile,O_RDWR|O_CREAT,0644);
    if(filefd <0){
        Log(LOG_ERROR, "save rdb file error, %s:%d,errno:%d",redis_c->ip,redis_c->port ,filefd);
        return 0;
    }
    char buf[1024];
    long  n ,left;
    contex->transfer_read = 0;
    while(contex->transfer_size > contex->transfer_read){
        left = contex->transfer_size - contex->transfer_read;
        if(left > 1024){
            left = 1024;
        }
        n = read(fd,buf,left);
        if(n ==0){
            Log(LOG_ERROR, "socket closed %s:%d",redis_c->ip,redis_c->port);
            return 0;
        }
        write(filefd,buf,n);
        contex->transfer_read += n;
    }
    Log(LOG_NOTICE, "save the rdb from server %s:%d done, the file is %s",redis_c->ip,redis_c->port ,contex->rdbfile);
    close(filefd);
    return 1;
}

void * parseRdbThread(void *data){
    server_contex *contex = data;
    pipe_server * sc = contex->sc;
    redis_conf *redis_c = array_get(contex->sc->servers_from, 0);
    Log(LOG_NOTICE, "begin parse the rdb from server %s:%d, the rdbfile is %s",redis_c->ip,redis_c->port,redis_c->rdbfile);
    contex->rdbfd = open(contex->rdbfile,O_RDWR);
    if(!processHeader(contex)){
        Log(LOG_ERROR, "parse header error from server %s:%d",redis_c->ip,redis_c->port);
        // exit(1);
    }

    if(!parseRdb(contex)){
        Log(LOG_ERROR, "parse rdb error from server %s:%d",redis_c->ip,redis_c->port);
        // exit(1);
    }

    Log(LOG_NOTICE, "parse the rdb from server %s:%d done , the rdbfile is %s ,processed %d",redis_c->ip,redis_c->port,contex->rdbfile,contex->processed);
    close(contex->rdbfd);
    unlink(contex->rdbfile);
}

//需要再看看redis协议
int processHeader(server_contex * contex) {
    redis_conf *redis_c = array_get(contex->sc->servers_from, 0);
    int rdbfd = contex->rdbfd;
    char buf[10] = "_________";
    int dump_version;

    if (!readBytes(rdbfd,buf, 9)) {
        Log(LOG_ERROR, "Cannot read header, server %s:%d, errno %s",redis_c->ip,redis_c->port,strerror(errno));
        return 0;
    }

    /* expect contexe first 5 bytes to equal REDIS */
    if (memcmp(buf,"REDIS",5) != 0) {
        Log(LOG_ERROR, "Wrong signature in header, server %s:%d",redis_c->ip,redis_c->port);
        return 0;
    }

    dump_version = (int)strtol(buf + 5, NULL, 10);
    if (dump_version < 1 || dump_version > 8) {
        Log(LOG_ERROR, "Unknown RDB format version: %d\n", dump_version);
        return 0;
    }
    contex->version = dump_version;
    Log(LOG_NOTICE, "redis version is %d, server %s:%d",dump_version,redis_c->ip,redis_c->port);
    contex->transfer_size-=9;
    return dump_version;
}

