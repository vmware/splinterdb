
.cache/invalid-address-perf-20261017/builds/async-mask/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000051d60 <clockcache_config_init>:
   51d60:	mov    %rsi,%r10
   51d63:	mov    %rcx,%rsi
   51d66:	push   %rbx
   51d67:	mov    0x8(%r10),%rcx
   51d6b:	test   %rcx,%rcx
   51d6e:	je     51e10 <clockcache_config_init+0xb0>
   51d74:	lea    -0x1(%rcx),%rax
   51d78:	and    %rcx,%rax
   51d7b:	jne    51e10 <clockcache_config_init+0xb0>
   51d81:	movq   $0x0,0x148(%rdi)
   51d8c:	mov    %rdi,%r9
   51d8f:	lea    0x8(%rdi),%rdi
   51d93:	lea    0x5f7e0(%rip),%rbx        # b157a <_fini+0x95e>
   51d9a:	and    $0xfffffffffffffff8,%rdi
   51d9e:	mov    %r9,%rcx
   51da1:	sub    %rdi,%rcx
   51da4:	add    $0x150,%ecx
   51daa:	shr    $0x3,%ecx
   51dad:	rep stos %rax,%es:(%rdi)
   51db0:	mov    0x831d1(%rip),%rax        # d4f88 <clockcache_config_ops@@Base-0x1148>
   51db7:	mov    %rdx,0x10(%r9)
   51dbb:	lea    0x1c(%r9),%rdi
   51dbf:	mov    %r10,0x8(%r9)
   51dc3:	mov    0x8(%r10),%rcx
   51dc7:	mov    %rax,(%r9)
   51dca:	mov    %r8d,0x18(%r9)
   51dce:	bsr    %rcx,%rax
   51dd2:	cltq
   51dd4:	mov    %rax,0x120(%r9)
   51ddb:	mov    %rdx,%rax
   51dde:	xor    %edx,%edx
   51de0:	div    %rcx
   51de3:	test   %rsi,%rsi
   51de6:	lea    0x60812(%rip),%rdx        # b25ff <_fini+0x19e3>
   51ded:	cmove  %rbx,%rsi
   51df1:	mov    %rsi,%rcx
   51df4:	mov    $0x100,%esi
   51df9:	mov    %eax,0x130(%r9)
   51e00:	xor    %eax,%eax
   51e02:	call   e1f0 <snprintf@plt>
   51e07:	cmp    $0xff,%eax
   51e0c:	jg     51e38 <clockcache_config_init+0xd8>
   51e0e:	pop    %rbx
   51e0f:	ret
   51e10:	lea    0x5f763(%rip),%r8        # b157a <_fini+0x95e>
   51e17:	lea    0x6661a(%rip),%rcx        # b8438 <__func__.21+0xfa8>
   51e1e:	xor    %eax,%eax
   51e20:	mov    $0x110b,%esi
   51e25:	lea    0x70864(%rip),%rdx        # c2690 <__func__.2>
   51e2c:	lea    0x5f7a4(%rip),%rdi        # b15d7 <_fini+0x9bb>
   51e33:	call   ee00 <platform_assert_false@plt>
   51e38:	mov    %rbx,%r8
   51e3b:	lea    0x5f81a(%rip),%rcx        # b165c <_fini+0xa40>
   51e42:	xor    %eax,%eax
   51e44:	mov    $0x1119,%esi
   51e49:	lea    0x70840(%rip),%rdx        # c2690 <__func__.2>
   51e50:	lea    0x5f780(%rip),%rdi        # b15d7 <_fini+0x9bb>
   51e57:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
