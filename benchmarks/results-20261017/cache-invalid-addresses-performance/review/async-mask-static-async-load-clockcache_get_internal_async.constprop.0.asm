
.cache/invalid-address-perf-20261017/builds/async-load/release/lib/libsplinterdb.so:     file format elf64-x86-64


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
   9cd50:	mov    %rsi,%rax
   9cd53:	xor    %edx,%edx
   9cd55:	mov    0x8(%rdi),%rcx
   9cd59:	mov    0x8(%rcx),%r10
   9cd5d:	divq   0x8(%r10)
   9cd61:	test   %rdx,%rdx
   9cd64:	jne    10534 <clockcache_get_internal_async.constprop.0.cold>
   9cd6a:	mov    0x120(%rcx),%rcx
   9cd71:	mov    0x28(%rdi),%rax
   9cd75:	mov    %rsi,%rdx
   9cd78:	shr    %cl,%rdx
   9cd7b:	shr    $0x2,%rax
   9cd7f:	cmp    %rax,%rdx
   9cd82:	jae    10534 <clockcache_get_internal_async.constprop.0.cold>
   9cd88:	mov    0x30(%rdi),%rax
   9cd8c:	mov    (%rax,%rdx,4),%eax
   9cd8f:	mov    $0xffffffff,%edx
   9cd94:	mov    %rax,0x60(%rbx)
   9cd98:	cmp    %rdx,%rax
   9cd9b:	je     9cdd0 <clockcache_get_internal_async.constprop.0+0xd0>
   9cd9d:	lea    0xc(%rip),%rax        # 9cdb0 <clockcache_get_internal_async.constprop.0+0xb0>
   9cda4:	movq   $0x0,0x18(%rbx)
   9cdac:	mov    %rax,0x10(%rbx)
   9cdb0:	mov    %rbx,%rdi
   9cdb3:	call   aa540 <clockcache_get_in_cache_async.constprop.0>
   9cdb8:	cmp    $0x1,%eax
   9cdbb:	jne    9cdfd <clockcache_get_internal_async.constprop.0+0xfd>
   9cdbd:	movq   $0x1,0x10(%rbx)
   9cdc5:	mov    $0x1,%eax
   9cdca:	pop    %rbx
   9cdcb:	ret
   9cdcc:	nopl   0x0(%rax)
   9cdd0:	lea    0x19(%rip),%rax        # 9cdf0 <clockcache_get_internal_async.constprop.0+0xf0>
   9cdd7:	movq   $0x0,0x18(%rbx)
   9cddf:	mov    %rax,0x10(%rbx)
   9cde3:	data16 cs nopw 0x0(%rax,%rax,1)
   9cdee:	xchg   %ax,%ax
   9cdf0:	mov    %rbx,%rdi
   9cdf3:	call   9c750 <clockcache_get_from_disk_async.constprop.0>
   9cdf8:	cmp    $0x1,%eax
   9cdfb:	je     9cdbd <clockcache_get_internal_async.constprop.0+0xbd>
   9cdfd:	xor    %eax,%eax
   9cdff:	pop    %rbx
   9ce00:	ret
   9ce01:	nopl   0x0(%rax)
   9ce08:	mov    $0x1,%eax
   9ce0d:	ret
   9ce0e:	push   %rax
   9ce0f:	lea    0x1b82a(%rip),%r8        # b8640 <__func__.21+0x11b0>
   9ce16:	xor    %eax,%eax
   9ce18:	lea    0x1b849(%rip),%rcx        # b8668 <__func__.21+0x11d8>
   9ce1f:	push   %rsi
   9ce20:	lea    0x25a79(%rip),%rdx        # c28a0 <__func__.23.lto_priv.1>
   9ce27:	mov    $0x1d6,%esi
   9ce2c:	lea    0x147a4(%rip),%rdi        # b15d7 <_fini+0x9bb>
   9ce33:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
