
.cache/invalid-address-perf-20261017/builds/outlined/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

000000000001057b <clockcache_get_internal_async.constprop.0.cold>:
   1057b:	call   ff5b <clockcache_report_invalid_address.lto_priv.0>
   10580:	mov    0xb2619(%rip),%rax        # c2ba0 <__func__.0.lto_priv.4+0x60>
   10587:	movq   $0x1,0x10(%rbx)
   1058f:	mov    %rax,0x50(%rbx)
   10593:	jmp    9cd68 <clockcache_get_internal_async.constprop.0+0xc8>

Disassembly of section .fini:
