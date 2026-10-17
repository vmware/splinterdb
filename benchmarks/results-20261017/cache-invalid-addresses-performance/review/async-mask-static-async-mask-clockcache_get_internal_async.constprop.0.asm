
.cache/invalid-address-perf-20261017/builds/async-mask/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

000000000009cd00 <clockcache_get_internal_async.constprop.0>:
   9cd00:	mov    0x10(%rdi),%rax
   9cd04:	cmp    $0x1,%rax
   9cd08:	je     9ce08 <clockcache_get_internal_async.constprop.0+0x108>
   9cd0e:	push   %rbx
   9cd0f:	mov    %rdi,%rbx
   9cd12:	test   %rax,%rax
   9cd15:	je     9cd20 <clockcache_get_internal_async.constprop.0+0x20>
   9cd17:	jmp    *%rax
   9cd19:	nopl   0x0(%rax)
   9cd20:	data16 lea 0x38280(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   9cd28:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   9cd30:	mov    0x30(%rbx),%r9d
   9cd34:	mov    0x28(%rbx),%rsi
   9cd38:	mov    (%rax),%rax
   9cd3b:	mov    %rax,0x58(%rbx)
   9cd3f:	lea    -0x1(%r9),%eax
   9cd43:	cmp    $0x7,%eax
   9cd46:	ja     9ce0e <clockcache_get_internal_async.constprop.0+0x10e>
   9cd4c:	mov    0x20(%rbx),%rdi
   9cd50:	mov    0x8(%rdi),%rdx
   9cd54:	mov    0x8(%rdx),%rax
   9cd58:	mov    0x8(%rax),%rax
   9cd5c:	sub    $0x1,%rax
   9cd60:	test   %rsi,%rax
   9cd63:	jne    1053a <clockcache_get_internal_async.constprop.0.cold>
   9cd69:	mov    0x120(%rdx),%rcx
   9cd70:	mov    0x28(%rdi),%rax
   9cd74:	mov    %rsi,%rdx
   9cd77:	shr    %cl,%rdx
   9cd7a:	shr    $0x2,%rax
   9cd7e:	cmp    %rax,%rdx
   9cd81:	jae    1053a <clockcache_get_internal_async.constprop.0.cold>
   9cd87:	mov    0x30(%rdi),%rax
   9cd8b:	mov    (%rax,%rdx,4),%eax
   9cd8e:	mov    $0xffffffff,%edx
   9cd93:	mov    %rax,0x60(%rbx)
   9cd97:	cmp    %rdx,%rax
   9cd9a:	je     9cdd0 <clockcache_get_internal_async.constprop.0+0xd0>
   9cd9c:	lea    0xc(%rip),%rax        # 9cdaf <clockcache_get_internal_async.constprop.0+0xaf>
   9cda3:	movq   $0x0,0x18(%rbx)
   9cdab:	mov    %rax,0x10(%rbx)
   9cdaf:	mov    %rbx,%rdi
   9cdb2:	call   aa540 <clockcache_get_in_cache_async.constprop.0>
   9cdb7:	cmp    $0x1,%eax
   9cdba:	jne    9cdfd <clockcache_get_internal_async.constprop.0+0xfd>
   9cdbc:	movq   $0x1,0x10(%rbx)
   9cdc4:	mov    $0x1,%eax
   9cdc9:	pop    %rbx
   9cdca:	ret
   9cdcb:	nopl   0x0(%rax,%rax,1)
   9cdd0:	lea    0x19(%rip),%rax        # 9cdf0 <clockcache_get_internal_async.constprop.0+0xf0>
   9cdd7:	movq   $0x0,0x18(%rbx)
   9cddf:	mov    %rax,0x10(%rbx)
   9cde3:	data16 cs nopw 0x0(%rax,%rax,1)
   9cdee:	xchg   %ax,%ax
   9cdf0:	mov    %rbx,%rdi
   9cdf3:	call   9c750 <clockcache_get_from_disk_async.constprop.0>
   9cdf8:	cmp    $0x1,%eax
   9cdfb:	je     9cdbc <clockcache_get_internal_async.constprop.0+0xbc>
   9cdfd:	xor    %eax,%eax
   9cdff:	pop    %rbx
   9ce00:	ret
   9ce01:	nopl   0x0(%rax)
   9ce08:	mov    $0x1,%eax
   9ce0d:	ret
   9ce0e:	push   %rax
   9ce0f:	lea    0x1b852(%rip),%r8        # b8668 <__func__.21+0x11d8>
   9ce16:	xor    %eax,%eax
   9ce18:	lea    0x1b871(%rip),%rcx        # b8690 <__func__.21+0x1200>
   9ce1f:	push   %rsi
   9ce20:	lea    0x25a99(%rip),%rdx        # c28c0 <__func__.23.lto_priv.1>
   9ce27:	mov    $0x1d6,%esi
   9ce2c:	lea    0x147a4(%rip),%rdi        # b15d7 <_fini+0x9bb>
   9ce33:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
