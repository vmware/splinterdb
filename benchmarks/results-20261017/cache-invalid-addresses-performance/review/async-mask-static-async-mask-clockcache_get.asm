
.cache/invalid-address-perf-20261017/builds/async-mask/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000053a50 <clockcache_get>:
   53a50:	push   %r15
   53a52:	lea    -0x1(%rcx),%eax
   53a55:	push   %r14
   53a57:	push   %r13
   53a59:	push   %r12
   53a5b:	push   %rbp
   53a5c:	push   %rbx
   53a5d:	mov    %rsi,%rbx
   53a60:	sub    $0x48,%rsp
   53a64:	mov    %edx,0x8(%rsp)
   53a68:	mov    %ecx,0x1c(%rsp)
   53a6c:	mov    %r8,0x20(%rsp)
   53a71:	cmp    $0x7,%eax
   53a74:	ja     54068 <clockcache_get+0x618>
   53a7a:	mov    0x8(%rdi),%rdx
   53a7e:	mov    %rdi,%r14
   53a81:	mov    0x8(%rdx),%rax
   53a85:	mov    0x8(%rax),%rax
   53a89:	sub    $0x1,%rax
   53a8d:	test   %rsi,%rax
   53a90:	jne    10163 <clockcache_get.cold>
   53a96:	movabs $0xcccccccccccccccd,%r12
   53aa0:	mov    %rdi,%r13
   53aa3:	data16 cs nopw 0x0(%rax,%rax,1)
   53aae:	xchg   %ax,%ax
   53ab0:	mov    0x120(%rdx),%rcx
   53ab7:	mov    0x28(%r13),%rax
   53abb:	mov    %rbx,%rdx
   53abe:	shr    %cl,%rdx
   53ac1:	shr    $0x2,%rax
   53ac5:	cmp    %rax,%rdx
   53ac8:	jae    540cb <clockcache_get+0x67b>
   53ace:	mov    0x30(%r13),%rax
   53ad2:	mov    (%rax,%rdx,4),%ebp
   53ad5:	cmp    $0xffffffff,%ebp
   53ad8:	je     53c80 <clockcache_get+0x230>
   53ade:	data16 lea 0x814c2(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53ae6:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53aee:	mov    0x8(%rsp),%r14d
   53af3:	mov    %ebp,%esi
   53af5:	mov    %r13,%rdi
   53af8:	mov    (%rax),%rax
   53afb:	mov    %rax,0x10(%rsp)
   53b00:	test   %r14d,%r14d
   53b03:	je     53c40 <clockcache_get+0x1f0>
   53b09:	call   aa3e0 <clockcache_try_get_read.constprop.0>
   53b0e:	cmp    $0x1,%eax
   53b11:	jne    53b5d <clockcache_get+0x10d>
   53b13:	mov    $0x1,%r15d
   53b19:	mov    %r15,%rax
   53b1c:	mul    %r12
   53b1f:	xor    %eax,%eax
   53b21:	shr    $0x2,%rdx
   53b25:	mov    %rdx,%r14
   53b28:	nopl   0x0(%rax,%rax,1)
   53b30:	pause
   53b32:	mov    %rax,%rcx
   53b35:	add    $0x1,%rax
   53b39:	cmp    %rcx,%r14
   53b3c:	jne    53b30 <clockcache_get+0xe0>
   53b3e:	cmp    $0x400,%r15
   53b45:	ja     53c10 <clockcache_get+0x1c0>
   53b4b:	mov    %ebp,%esi
   53b4d:	mov    %r13,%rdi
   53b50:	add    %r15,%r15
   53b53:	call   aa3e0 <clockcache_try_get_read.constprop.0>
   53b58:	cmp    $0x1,%eax
   53b5b:	je     53b19 <clockcache_get+0xc9>
   53b5d:	test   %eax,%eax
   53b5f:	jne    53c57 <clockcache_get+0x207>
   53b65:	mov    %ebp,%eax
   53b67:	mov    0x48(%r13),%rcx
   53b6b:	mov    %rax,%rdx
   53b6e:	shl    $0x6,%rdx
   53b72:	mov    0x8(%rcx,%rdx,1),%rsi
   53b77:	cmp    %rsi,%rbx
   53b7a:	jne    53ebc <clockcache_get+0x46c>
   53b80:	mov    %rax,%rbx
   53b83:	mov    %rdx,%r15
   53b86:	mov    %r13,%r14
   53b89:	jmp    53b9c <clockcache_get+0x14c>
   53b8b:	nopl   0x0(%rax,%rax,1)
   53b90:	mov    %r14,%rdi
   53b93:	call   ea40 <clockcache_wait@plt>
   53b98:	mov    0x48(%r14),%rcx
   53b9c:	mov    0x10(%rcx,%r15,1),%eax
   53ba1:	test   $0x10,%al
   53ba3:	jne    53b90 <clockcache_get+0x140>
   53ba5:	mov    0x48(%r14),%rdx
   53ba9:	mov    %rbx,%rax
   53bac:	add    %r15,%rdx
   53baf:	mov    0x14(%rdx),%r13d
   53bb3:	test   %r13d,%r13d
   53bb6:	jne    5401a <clockcache_get+0x5ca>
   53bbc:	mov    0x20(%rdx),%eax
   53bbf:	cmp    %eax,0x1c(%rsp)
   53bc3:	jne    5409a <clockcache_get+0x64a>
   53bc9:	mov    0x8(%r14),%rax
   53bcd:	mov    0x18(%rax),%r11d
   53bd1:	test   %r11d,%r11d
   53bd4:	jne    53e96 <clockcache_get+0x446>
   53bda:	mov    0x20(%rsp),%rbx
   53bdf:	xor    %r13d,%r13d
   53be2:	mov    %rdx,(%rbx)
   53be5:	data16 lea 0x813bb(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53bed:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53bf5:	mov    (%rax),%rax
   53bf8:	mov    %rax,0x8(%rbx)
   53bfc:	add    $0x48,%rsp
   53c00:	mov    %r13d,%eax
   53c03:	pop    %rbx
   53c04:	pop    %rbp
   53c05:	pop    %r12
   53c07:	pop    %r13
   53c09:	pop    %r14
   53c0b:	pop    %r15
   53c0d:	ret
   53c0e:	xchg   %ax,%ax
   53c10:	mov    %ebp,%esi
   53c12:	mov    %r13,%rdi
   53c15:	call   aa3e0 <clockcache_try_get_read.constprop.0>
   53c1a:	cmp    $0x1,%eax
   53c1d:	jne    53b5d <clockcache_get+0x10d>
   53c23:	xor    %eax,%eax
   53c25:	data16 cs nopw 0x0(%rax,%rax,1)
   53c30:	pause
   53c32:	mov    %rax,%rdx
   53c35:	add    $0x1,%rax
   53c39:	cmp    %rdx,%r14
   53c3c:	jne    53c30 <clockcache_get+0x1e0>
   53c3e:	jmp    53c10 <clockcache_get+0x1c0>
   53c40:	call   aa3e0 <clockcache_try_get_read.constprop.0>
   53c45:	cmp    $0x1,%eax
   53c48:	je     53cd6 <clockcache_get+0x286>
   53c4e:	cmp    $0x2,%eax
   53c51:	jne    53b65 <clockcache_get+0x115>
   53c57:	mov    0x8(%r13),%rdx
   53c5b:	mov    0x8(%rdx),%rax
   53c5f:	mov    0x8(%rax),%rax
   53c63:	sub    $0x1,%rax
   53c67:	test   %rbx,%rax
   53c6a:	je     53ab0 <clockcache_get+0x60>
   53c70:	mov    %r13,%r14
   53c73:	jmp    10163 <clockcache_get.cold>
   53c78:	nopl   0x0(%rax,%rax,1)
   53c80:	mov    0x10(%r13),%rbp
   53c84:	mov    0x0(%rbp),%rax
   53c88:	mov    %rbp,%rdi
   53c8b:	call   *(%rax)
   53c8d:	xor    %edx,%edx
   53c8f:	mov    %rax,%rcx
   53c92:	mov    (%rax),%rax
   53c95:	mov    0x8(%rax),%r9
   53c99:	mov    %rbx,%rax
   53c9c:	div    %r9
   53c9f:	test   %rdx,%rdx
   53ca2:	jne    53e60 <clockcache_get+0x410>
   53ca8:	mov    0x0(%rbp),%rax
   53cac:	mov    %rbx,%r15
   53caf:	and    0x20(%rcx),%r15
   53cb3:	je     53ce8 <clockcache_get+0x298>
   53cb5:	cmp    0x8(%rcx),%rbx
   53cb9:	jae    53ce8 <clockcache_get+0x298>
   53cbb:	mov    %r15,%rsi
   53cbe:	mov    %rbp,%rdi
   53cc1:	call   *0x20(%rax)
   53cc4:	test   %eax,%eax
   53cc6:	je     53f10 <clockcache_get+0x4c0>
   53ccc:	mov    0x8(%rsp),%r8d
   53cd1:	test   %r8d,%r8d
   53cd4:	jne    53d34 <clockcache_get+0x2e4>
   53cd6:	mov    $0xb,%r13d
   53cdc:	jmp    53bfc <clockcache_get+0x1ac>
   53ce1:	nopl   0x0(%rax)
   53ce8:	mov    %rbp,%rdi
   53ceb:	call   *0x58(%rax)
   53cee:	mov    0x812db(%rip),%rbp        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   53cf5:	mov    %rbx,%r9
   53cf8:	mov    %r15,%r8
   53cfb:	sub    $0x8,%rsp
   53cff:	lea    0x6385a(%rip),%rsi        # b7560 <__func__.21+0xd0>
   53d06:	mov    $0x17d,%ecx
   53d0b:	mov    0x0(%rbp),%rdi
   53d0f:	push   %rax
   53d10:	lea    0x6ebd9(%rip),%rdx        # c28f0 <__func__.22.lto_priv.0>
   53d17:	xor    %eax,%eax
   53d19:	call   fbf0 <fprintf@plt>
   53d1e:	mov    0x0(%rbp),%rdi
   53d22:	call   fe90 <fflush@plt>
   53d27:	pop    %rsi
   53d28:	pop    %rdi
   53d29:	mov    $0x16,%r13d
   53d2f:	jmp    53bfc <clockcache_get+0x1ac>
   53d34:	data16 lea 0x8126c(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53d3c:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53d44:	mov    0x1c(%rsp),%edx
   53d48:	mov    %rbx,%rsi
   53d4b:	mov    %r13,%rdi
   53d4e:	mov    (%rax),%r14
   53d51:	mov    0x8(%r13),%rax
   53d55:	mov    0x8(%rax),%rax
   53d59:	mov    0x8(%rax),%r15
   53d5d:	call   536f0 <clockcache_acquire_entry_for_load.lto_priv.0>
   53d62:	mov    %rax,%rbp
   53d65:	mov    $0xffffffff,%eax
   53d6a:	cmp    %rax,%rbp
   53d6d:	je     53c57 <clockcache_get+0x207>
   53d73:	mov    0x8(%r13),%rax
   53d77:	mov    %rbp,%r12
   53d7a:	mov    %r14,0x8(%rsp)
   53d7f:	mov    %r13,%r14
   53d82:	shl    $0x6,%r12
   53d86:	add    0x48(%r13),%r12
   53d8a:	mov    0x18(%rax),%ecx
   53d8d:	test   %ecx,%ecx
   53d8f:	jne    53fae <clockcache_get+0x55e>
   53d95:	mov    0x18(%r14),%rdi
   53d99:	mov    (%r12),%rsi
   53d9d:	mov    %rbx,%rcx
   53da0:	mov    %r15,%rdx
   53da3:	mov    (%rdi),%rax
   53da6:	call   *(%rax)
   53da8:	mov    %eax,%r13d
   53dab:	test   %eax,%eax
   53dad:	jne    53fd5 <clockcache_get+0x585>
   53db3:	mov    0x8(%r14),%rax
   53db7:	mov    0x18(%rax),%edx
   53dba:	test   %edx,%edx
   53dbc:	jne    53f51 <clockcache_get+0x501>
   53dc2:	mov    %ebp,%edx
   53dc4:	mov    %r13d,%ecx
   53dc7:	mov    %rbx,%rsi
   53dca:	mov    %r14,%rdi
   53dcd:	call   53820 <clockcache_finish_load.lto_priv.0>
   53dd2:	mov    %r12,%rdx
   53dd5:	test   %r13d,%r13d
   53dd8:	je     53bda <clockcache_get+0x18a>
   53dde:	mov    0x8(%r14),%rcx
   53de2:	mov    %rbp,%rax
   53de5:	xor    %edx,%edx
   53de7:	mov    0x8(%rsp),%rdi
   53dec:	divq   0x140(%rcx)
   53df3:	and    $0x3,%edi
   53df6:	shl    $0x6,%edx
   53df9:	lea    (%rdx,%rax,1),%esi
   53dfc:	mov    0x130(%rcx),%eax
   53e02:	mov    0x88(%r14),%rdx
   53e09:	mov    $0xffffffff,%ecx
   53e0e:	imul   %rdi,%rax
   53e12:	add    %rsi,%rax
   53e15:	lock xadd %cx,(%rdx,%rax,2)
   53e1b:	test   %cx,%cx
   53e1e:	jne    53bfc <clockcache_get+0x1ac>
   53e24:	push   %rax
   53e25:	push   %rdi
   53e26:	push   %rsi
   53e27:	push   0x20(%rsp)
   53e2b:	xor    %r9d,%r9d
   53e2e:	lea    0x643c3(%rip),%r8        # b81f8 <__func__.21+0xd68>
   53e35:	xor    %eax,%eax
   53e37:	mov    $0x288,%esi
   53e3c:	lea    0x5d7eb(%rip),%rcx        # b162e <_fini+0xa12>
   53e43:	lea    0x6ebe6(%rip),%rdx        # c2a30 <__func__.28.lto_priv.1>
   53e4a:	lea    0x5d786(%rip),%rdi        # b15d7 <_fini+0x9bb>
   53e51:	call   ee00 <platform_assert_false@plt>
   53e56:	cs nopw 0x0(%rax,%rax,1)
   53e60:	mov    0x81169(%rip),%rbp        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   53e67:	mov    %rbx,%r8
   53e6a:	xor    %eax,%eax
   53e6c:	mov    $0x164,%ecx
   53e71:	lea    0x6ea78(%rip),%rdx        # c28f0 <__func__.22.lto_priv.0>
   53e78:	lea    0x63629(%rip),%rsi        # b74a8 <__func__.21+0x18>
   53e7f:	mov    0x0(%rbp),%rdi
   53e83:	call   fbf0 <fprintf@plt>
   53e88:	mov    0x0(%rbp),%rdi
   53e8c:	call   fe90 <fflush@plt>
   53e91:	jmp    53d29 <clockcache_get+0x2d9>
   53e96:	mov    0x10(%rsp),%rsi
   53e9b:	mov    0x1c(%rsp),%ecx
   53e9f:	lea    0x0(,%rsi,8),%rax
   53ea7:	sub    %rsi,%rax
   53eaa:	lea    0x220(%rcx,%rax,8),%rax
   53eb2:	addq   $0x1,(%r14,%rax,8)
   53eb7:	jmp    53bda <clockcache_get+0x18a>
   53ebc:	mov    0x8(%r13),%rcx
   53ec0:	xor    %edx,%edx
   53ec2:	mov    0x10(%rsp),%rdi
   53ec7:	divq   0x140(%rcx)
   53ece:	and    $0x3,%edi
   53ed1:	shl    $0x6,%edx
   53ed4:	lea    (%rdx,%rax,1),%esi
   53ed7:	mov    0x130(%rcx),%eax
   53edd:	mov    0x88(%r13),%rdx
   53ee4:	mov    $0xffffffff,%ecx
   53ee9:	imul   %rdi,%rax
   53eed:	add    %rsi,%rax
   53ef0:	lock xadd %cx,(%rdx,%rax,2)
   53ef6:	test   %cx,%cx
   53ef9:	jne    53c57 <clockcache_get+0x207>
   53eff:	push   %rbx
   53f00:	push   %rdi
   53f01:	push   %rsi
   53f02:	push   0x28(%rsp)
   53f06:	jmp    53e2b <clockcache_get+0x3db>
   53f0b:	nopl   0x0(%rax,%rax,1)
   53f10:	sub    $0x8,%rsp
   53f14:	mov    %rbx,%r9
   53f17:	mov    %r15,%r8
   53f1a:	mov    $0x172,%ecx
   53f1f:	mov    0x810aa(%rip),%rbp        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   53f26:	lea    0x6e9c3(%rip),%rdx        # c28f0 <__func__.22.lto_priv.0>
   53f2d:	lea    0x635c4(%rip),%rsi        # b74f8 <__func__.21+0x68>
   53f34:	mov    0x0(%rbp),%rdi
   53f38:	push   $0x0
   53f3a:	call   fbf0 <fprintf@plt>
   53f3f:	mov    0x0(%rbp),%rdi
   53f43:	call   fe90 <fflush@plt>
   53f48:	pop    %r9
   53f4a:	pop    %r10
   53f4c:	jmp    53d29 <clockcache_get+0x2d9>
   53f51:	lea    0x30(%rsp),%rsi
   53f56:	mov    $0x1,%edi
   53f5b:	call   e410 <clock_gettime@plt>
   53f60:	mov    0x28(%rsp),%rax
   53f65:	mov    0x8(%rsp),%rsi
   53f6a:	imul   $0x3b9aca00,0x30(%rsp),%rdx
   53f73:	add    0x38(%rsp),%rdx
   53f78:	sub    %rax,%rdx
   53f7b:	mov    0x1c(%rsp),%ecx
   53f7f:	lea    0x0(,%rsi,8),%rax
   53f87:	sub    %rsi,%rax
   53f8a:	lea    (%rcx,%rax,8),%rax
   53f8e:	lea    (%r14,%rax,8),%rax
   53f92:	addq   $0x1,0x1148(%rax)
   53f9a:	addq   $0x1,0x1220(%rax)
   53fa2:	add    %rdx,0x1190(%rax)
   53fa9:	jmp    53dc2 <clockcache_get+0x372>
   53fae:	lea    0x30(%rsp),%rsi
   53fb3:	mov    $0x1,%edi
   53fb8:	call   e410 <clock_gettime@plt>
   53fbd:	imul   $0x3b9aca00,0x30(%rsp),%rax
   53fc6:	add    0x38(%rsp),%rax
   53fcb:	mov    %rax,0x28(%rsp)
   53fd0:	jmp    53d95 <clockcache_get+0x345>
   53fd5:	mov    %eax,%edi
   53fd7:	call   efe0 <strerror@plt>
   53fdc:	mov    0x80fed(%rip),%r15        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   53fe3:	mov    0x1c(%rsp),%ecx
   53fe7:	mov    %rbx,%rdx
   53fea:	mov    %rax,%r8
   53fed:	lea    0x648d4(%rip),%rsi        # b88c8 <__func__.21+0x1438>
   53ff4:	xor    %eax,%eax
   53ff6:	mov    (%r15),%rdi
   53ff9:	call   fbf0 <fprintf@plt>
   53ffe:	mov    (%r15),%rdi
   54001:	call   fe90 <fflush@plt>
   54006:	mov    0x8(%r14),%rax
   5400a:	mov    0x18(%rax),%edx
   5400d:	test   %edx,%edx
   5400f:	je     53dc2 <clockcache_get+0x372>
   54015:	jmp    53f51 <clockcache_get+0x501>
   5401a:	mov    0x8(%r14),%rcx
   5401e:	xor    %edx,%edx
   54020:	mov    0x10(%rsp),%rdi
   54025:	divq   0x140(%rcx)
   5402c:	and    $0x3,%edi
   5402f:	shl    $0x6,%edx
   54032:	lea    (%rdx,%rax,1),%esi
   54035:	mov    0x130(%rcx),%eax
   5403b:	mov    0x88(%r14),%rdx
   54042:	mov    $0xffffffff,%ecx
   54047:	imul   %rdi,%rax
   5404b:	add    %rsi,%rax
   5404e:	lock xadd %cx,(%rdx,%rax,2)
   54054:	test   %cx,%cx
   54057:	jne    53bfc <clockcache_get+0x1ac>
   5405d:	jmp    53eff <clockcache_get+0x4af>
   54062:	nopw   0x0(%rax,%rax,1)
   54068:	sub    $0x8,%rsp
   5406c:	lea    0x645f5(%rip),%r8        # b8668 <__func__.21+0x11d8>
   54073:	xor    %eax,%eax
   54075:	push   %rsi
   54076:	mov    0x2c(%rsp),%r9d
   5407b:	lea    0x6460e(%rip),%rcx        # b8690 <__func__.21+0x1200>
   54082:	lea    0x6e837(%rip),%rdx        # c28c0 <__func__.23.lto_priv.1>
   54089:	mov    $0x1d6,%esi
   5408e:	lea    0x5d542(%rip),%rdi        # b15d7 <_fini+0x9bb>
   54095:	call   ee00 <platform_assert_false@plt>
   5409a:	mov    0x1c(%rsp),%esi
   5409e:	mov    %ebp,%r9d
   540a1:	lea    0x5d662(%rip),%r8        # b170a <_fini+0xaee>
   540a8:	lea    0x5d672(%rip),%rcx        # b1721 <_fini+0xb05>
   540af:	lea    0x6e71a(%rip),%rdx        # c27d0 <__func__.19>
   540b6:	lea    0x5d51a(%rip),%rdi        # b15d7 <_fini+0x9bb>
   540bd:	push   %rsi
   540be:	mov    $0x8dd,%esi
   540c3:	push   %rax
   540c4:	xor    %eax,%eax
   540c6:	call   ee00 <platform_assert_false@plt>
   540cb:	mov    %r13,%r14
   540ce:	jmp    10163 <clockcache_get.cold>

Disassembly of section .fini:
