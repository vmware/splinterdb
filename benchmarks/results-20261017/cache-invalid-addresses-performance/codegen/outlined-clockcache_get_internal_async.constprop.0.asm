
.cache/invalid-address-perf-20261017/builds/outlined/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

000000000009cca0 <clockcache_get_internal_async.constprop.0>:
   9cca0:	mov    0x10(%rdi),%rax
   9cca4:	cmp    $0x1,%rax
   9cca8:	je     9cda8 <clockcache_get_internal_async.constprop.0+0x108>
   9ccae:	push   %rbx
   9ccaf:	mov    %rdi,%rbx
   9ccb2:	test   %rax,%rax
   9ccb5:	je     9ccc0 <clockcache_get_internal_async.constprop.0+0x20>
   9ccb7:	jmp    *%rax
   9ccb9:	nopl   0x0(%rax)
   9ccc0:	data16 lea 0x382e0(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   9ccc8:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   9ccd0:	mov    0x30(%rbx),%r9d
   9ccd4:	mov    0x28(%rbx),%rsi
   9ccd8:	mov    (%rax),%rax
   9ccdb:	mov    %rax,0x58(%rbx)
   9ccdf:	lea    -0x1(%r9),%eax
   9cce3:	cmp    $0x7,%eax
   9cce6:	ja     9cdc5 <clockcache_get_internal_async.constprop.0+0x125>
   9ccec:	mov    0x20(%rbx),%rdi
   9ccf0:	mov    %rsi,%rdx
   9ccf3:	mov    0x8(%rdi),%rax
   9ccf7:	mov    0x120(%rax),%rcx
   9ccfe:	mov    0x8(%rax),%r10
   9cd02:	mov    %rsi,%rax
   9cd05:	shr    %cl,%rdx
   9cd08:	mov    %rdx,%rcx
   9cd0b:	xor    %edx,%edx
   9cd0d:	divq   0x8(%r10)
   9cd11:	test   %rdx,%rdx
   9cd14:	jne    1057b <clockcache_get_internal_async.constprop.0.cold>
   9cd1a:	mov    0x28(%rdi),%rax
   9cd1e:	shr    $0x2,%rax
   9cd22:	cmp    %rax,%rcx
   9cd25:	jae    1057b <clockcache_get_internal_async.constprop.0.cold>
   9cd2b:	mov    0x30(%rdi),%rax
   9cd2f:	mov    $0xffffffff,%edx
   9cd34:	mov    (%rax,%rcx,4),%eax
   9cd37:	mov    %rax,0x60(%rbx)
   9cd3b:	cmp    %rdx,%rax
   9cd3e:	je     9cd70 <clockcache_get_internal_async.constprop.0+0xd0>
   9cd40:	lea    0xc(%rip),%rax        # 9cd53 <clockcache_get_internal_async.constprop.0+0xb3>
   9cd47:	movq   $0x0,0x18(%rbx)
   9cd4f:	mov    %rax,0x10(%rbx)
   9cd53:	mov    %rbx,%rdi
   9cd56:	call   aa500 <clockcache_get_in_cache_async.constprop.0>
   9cd5b:	cmp    $0x1,%eax
   9cd5e:	jne    9cd9d <clockcache_get_internal_async.constprop.0+0xfd>
   9cd60:	movq   $0x1,0x10(%rbx)
   9cd68:	mov    $0x1,%eax
   9cd6d:	pop    %rbx
   9cd6e:	ret
   9cd6f:	nop
   9cd70:	call   50100 <clockcache_page_valid_for_load.lto_priv.0>
   9cd75:	test   %eax,%eax
   9cd77:	je     9cdb0 <clockcache_get_internal_async.constprop.0+0x110>
   9cd79:	lea    0x10(%rip),%rax        # 9cd90 <clockcache_get_internal_async.constprop.0+0xf0>
   9cd80:	movq   $0x0,0x18(%rbx)
   9cd88:	mov    %rax,0x10(%rbx)
   9cd8c:	nopl   0x0(%rax)
   9cd90:	mov    %rbx,%rdi
   9cd93:	call   9c850 <clockcache_get_from_disk_async.constprop.0>
   9cd98:	cmp    $0x1,%eax
   9cd9b:	je     9cd60 <clockcache_get_internal_async.constprop.0+0xc0>
   9cd9d:	xor    %eax,%eax
   9cd9f:	pop    %rbx
   9cda0:	ret
   9cda1:	nopl   0x0(%rax)
   9cda8:	mov    $0x1,%eax
   9cdad:	ret
   9cdae:	xchg   %ax,%ax
   9cdb0:	mov    0x25de9(%rip),%rax        # c2ba0 <__func__.0.lto_priv.4+0x60>
   9cdb7:	movq   $0x1,0x10(%rbx)
   9cdbf:	mov    %rax,0x50(%rbx)
   9cdc3:	jmp    9cd68 <clockcache_get_internal_async.constprop.0+0xc8>
   9cdc5:	push   %rax
   9cdc6:	lea    0x1b873(%rip),%r8        # b8640 <__func__.21+0x11b0>
   9cdcd:	xor    %eax,%eax
   9cdcf:	lea    0x1b892(%rip),%rcx        # b8668 <__func__.21+0x11d8>
   9cdd6:	push   %rsi
   9cdd7:	lea    0x25aa2(%rip),%rdx        # c2880 <__func__.23.lto_priv.1>
   9cdde:	mov    $0x1da,%esi
   9cde3:	lea    0x147ed(%rip),%rdi        # b15d7 <_fini+0x9fb>
   9cdea:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
