
.cache/invalid-address-perf-20261017/builds/cold/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

000000000001057b <clockcache_get_internal_async.constprop.0.cold>:
   1057b:	mov    %rbp,%rsi
   1057e:	call   ff5b <clockcache_report_invalid_address.lto_priv.0>
   10583:	mov    0xb2616(%rip),%rax        # c2ba0 <__func__.0.lto_priv.4+0x60>
   1058a:	movq   $0x1,0x10(%rbx)
   10592:	mov    %rax,0x50(%rbx)
   10596:	jmp    aab74 <clockcache_get_internal_async.constprop.0+0xd4>

Disassembly of section .fini:
