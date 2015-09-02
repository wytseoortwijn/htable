#ifndef ATOMICS_H
#define ATOMICS_H

#ifndef CAS
#define CAS(ptr, old, new) (bupc_atomicU64_cswap_strict((ptr), (old), (new)))
#endif

#ifndef CAS32
#define CAS32(ptr, old, new) (bupc_atomicU32_cswap_strict((ptr), (old), (new)))
#endif

#endif

