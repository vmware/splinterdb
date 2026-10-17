
.cache/invalid-address-perf-20261017/builds/candidate/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

000000000009cbe0 <clockcache_get_internal_async.constprop.0>:
   9cbe0:	mov    0x10(%rdi),%rax
   9cbe4:	cmp    $0x1,%rax
   9cbe8:	je     9cce8 <clockcache_get_internal_async.constprop.0+0x108>
   9cbee:	push   %rbx
   9cbef:	mov    %rdi,%rbx
   9cbf2:	test   %rax,%rax
   9cbf5:	je     9cc00 <clockcache_get_internal_async.constprop.0+0x20>
   9cbf7:	jmp    *%rax
   9cbf9:	nopl   0x0(%rax)
   9cc00:	data16 lea 0x383a0(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   9cc08:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   9cc10:	mov    0x30(%rbx),%r9d
   9cc14:	mov    0x28(%rbx),%rsi
   9cc18:	mov    (%rax),%rax
   9cc1b:	mov    %rax,0x58(%rbx)
   9cc1f:	lea    -0x1(%r9),%eax
   9cc23:	cmp    $0x7,%eax
   9cc26:	ja     9cd05 <clockcache_get_internal_async.constprop.0+0x125>
   9cc2c:	mov    0x20(%rbx),%rdi
   9cc30:	mov    %rsi,%rax
   9cc33:	xor    %edx,%edx
   9cc35:	mov    0x8(%rdi),%rcx
   9cc39:	mov    0x8(%rcx),%r10
   9cc3d:	divq   0x8(%r10)
   9cc41:	test   %rdx,%rdx
   9cc44:	jne    10534 <clockcache_get_internal_async.constprop.0.cold>
   9cc4a:	mov    0x120(%rcx),%rcx
   9cc51:	mov    0x28(%rdi),%rax
   9cc55:	mov    %rsi,%rdx
   9cc58:	shr    %cl,%rdx
   9cc5b:	shr    $0x2,%rax
   9cc5f:	cmp    %rax,%rdx
   9cc62:	jae    1053e <clockcache_get_internal_async.constprop.0.cold+0xa>
   9cc68:	mov    0x30(%rdi),%rax
   9cc6c:	mov    (%rax,%rdx,4),%eax
   9cc6f:	mov    $0xffffffff,%edx
   9cc74:	mov    %rax,0x60(%rbx)
   9cc78:	cmp    %rdx,%rax
   9cc7b:	je     9ccb0 <clockcache_get_internal_async.constprop.0+0xd0>
   9cc7d:	lea    0xc(%rip),%rax        # 9cc90 <clockcache_get_internal_async.constprop.0+0xb0>
   9cc84:	movq   $0x0,0x18(%rbx)
   9cc8c:	mov    %rax,0x10(%rbx)
   9cc90:	mov    %rbx,%rdi
   9cc93:	call   aa440 <clockcache_get_in_cache_async.constprop.0>
   9cc98:	cmp    $0x1,%eax
   9cc9b:	jne    9ccdd <clockcache_get_internal_async.constprop.0+0xfd>
   9cc9d:	movq   $0x1,0x10(%rbx)
   9cca5:	mov    $0x1,%eax
   9ccaa:	pop    %rbx
   9ccab:	ret
   9ccac:	nopl   0x0(%rax)
   9ccb0:	call   50080 <clockcache_page_valid_for_load.lto_priv.0>
   9ccb5:	test   %eax,%eax
   9ccb7:	je     9ccf0 <clockcache_get_internal_async.constprop.0+0x110>
   9ccb9:	lea    0x10(%rip),%rax        # 9ccd0 <clockcache_get_internal_async.constprop.0+0xf0>
   9ccc0:	movq   $0x0,0x18(%rbx)
   9ccc8:	mov    %rax,0x10(%rbx)
   9cccc:	nopl   0x0(%rax)
   9ccd0:	mov    %rbx,%rdi
   9ccd3:	call   9c790 <clockcache_get_from_disk_async.constprop.0>
   9ccd8:	cmp    $0x1,%eax
   9ccdb:	je     9cc9d <clockcache_get_internal_async.constprop.0+0xbd>
   9ccdd:	xor    %eax,%eax
   9ccdf:	pop    %rbx
   9cce0:	ret
   9cce1:	nopl   0x0(%rax)
   9cce8:	mov    $0x1,%eax
   9cced:	ret
   9ccee:	xchg   %ax,%ax
   9ccf0:	mov    0x25ea9(%rip),%rax        # c2ba0 <__func__.0.lto_priv.4+0x60>
   9ccf7:	movq   $0x1,0x10(%rbx)
   9ccff:	mov    %rax,0x50(%rbx)
   9cd03:	jmp    9cca5 <clockcache_get_internal_async.constprop.0+0xc5>
   9cd05:	push   %rax
   9cd06:	lea    0x1b933(%rip),%r8        # b8640 <__func__.21+0x11b0>
   9cd0d:	xor    %eax,%eax
   9cd0f:	lea    0x1b952(%rip),%rcx        # b8668 <__func__.21+0x11d8>
   9cd16:	push   %rsi
   9cd17:	lea    0x25b62(%rip),%rdx        # c2880 <__func__.23.lto_priv.1>
   9cd1e:	mov    $0x1de,%esi
   9cd23:	lea    0x148ad(%rip),%rdi        # b15d7 <_fini+0xabb>
   9cd2a:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
