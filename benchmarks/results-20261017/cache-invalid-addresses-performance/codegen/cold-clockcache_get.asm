
.cache/invalid-address-perf-20261017/builds/cold/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000053ad0 <clockcache_get>:
   53ad0:	push   %r15
   53ad2:	lea    -0x1(%rcx),%eax
   53ad5:	push   %r14
   53ad7:	push   %r13
   53ad9:	push   %r12
   53adb:	push   %rbp
   53adc:	push   %rbx
   53add:	mov    %rsi,%rbx
   53ae0:	sub    $0x48,%rsp
   53ae4:	mov    %edx,0x8(%rsp)
   53ae8:	mov    %ecx,0x1c(%rsp)
   53aec:	mov    %r8,0x20(%rsp)
   53af1:	cmp    $0x7,%eax
   53af4:	ja     54108 <clockcache_get+0x638>
   53afa:	mov    0x8(%rdi),%rax
   53afe:	mov    %rdi,%r14
   53b01:	mov    %rsi,%rdi
   53b04:	xor    %edx,%edx
   53b06:	mov    0x8(%rax),%rsi
   53b0a:	mov    0x120(%rax),%rcx
   53b11:	mov    %rbx,%rax
   53b14:	divq   0x8(%rsi)
   53b18:	shr    %cl,%rdi
   53b1b:	mov    %rdi,%rcx
   53b1e:	test   %rdx,%rdx
   53b21:	jne    101b7 <clockcache_get.cold>
   53b27:	mov    0x28(%r14),%rax
   53b2b:	shr    $0x2,%rax
   53b2f:	cmp    %rax,%rdi
   53b32:	jae    101b7 <clockcache_get.cold>
   53b38:	movabs $0xcccccccccccccccd,%r12
   53b42:	mov    %r14,%r13
   53b45:	data16 cs nopw 0x0(%rax,%rax,1)
   53b50:	mov    0x30(%r13),%rax
   53b54:	mov    (%rax,%rcx,4),%ebp
   53b57:	cmp    $0xffffffff,%ebp
   53b5a:	je     53d20 <clockcache_get+0x250>
   53b60:	data16 lea 0x81440(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53b68:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53b70:	mov    0x8(%rsp),%r14d
   53b75:	mov    %ebp,%esi
   53b77:	mov    %r13,%rdi
   53b7a:	mov    (%rax),%rax
   53b7d:	mov    %rax,0x10(%rsp)
   53b82:	test   %r14d,%r14d
   53b85:	je     53cc0 <clockcache_get+0x1f0>
   53b8b:	call   aa1f0 <clockcache_try_get_read.constprop.0>
   53b90:	cmp    $0x1,%eax
   53b93:	jne    53bdd <clockcache_get+0x10d>
   53b95:	mov    $0x1,%r15d
   53b9b:	mov    %r15,%rax
   53b9e:	mul    %r12
   53ba1:	xor    %eax,%eax
   53ba3:	shr    $0x2,%rdx
   53ba7:	mov    %rdx,%r14
   53baa:	nopw   0x0(%rax,%rax,1)
   53bb0:	pause
   53bb2:	mov    %rax,%rcx
   53bb5:	add    $0x1,%rax
   53bb9:	cmp    %r14,%rcx
   53bbc:	jne    53bb0 <clockcache_get+0xe0>
   53bbe:	cmp    $0x400,%r15
   53bc5:	ja     53c90 <clockcache_get+0x1c0>
   53bcb:	mov    %ebp,%esi
   53bcd:	mov    %r13,%rdi
   53bd0:	add    %r15,%r15
   53bd3:	call   aa1f0 <clockcache_try_get_read.constprop.0>
   53bd8:	cmp    $0x1,%eax
   53bdb:	je     53b9b <clockcache_get+0xcb>
   53bdd:	test   %eax,%eax
   53bdf:	jne    53cd7 <clockcache_get+0x207>
   53be5:	mov    %ebp,%eax
   53be7:	mov    0x48(%r13),%rcx
   53beb:	mov    %rax,%rdx
   53bee:	shl    $0x6,%rdx
   53bf2:	mov    0x8(%rcx,%rdx,1),%rsi
   53bf7:	cmp    %rsi,%rbx
   53bfa:	jne    53f5c <clockcache_get+0x48c>
   53c00:	mov    %rax,%rbx
   53c03:	mov    %rdx,%r15
   53c06:	mov    %r13,%r14
   53c09:	jmp    53c1c <clockcache_get+0x14c>
   53c0b:	nopl   0x0(%rax,%rax,1)
   53c10:	mov    %r14,%rdi
   53c13:	call   ea40 <clockcache_wait@plt>
   53c18:	mov    0x48(%r14),%rcx
   53c1c:	mov    0x10(%rcx,%r15,1),%eax
   53c21:	test   $0x10,%al
   53c23:	jne    53c10 <clockcache_get+0x140>
   53c25:	mov    0x48(%r14),%rdx
   53c29:	mov    %rbx,%rax
   53c2c:	add    %r15,%rdx
   53c2f:	mov    0x14(%rdx),%r13d
   53c33:	test   %r13d,%r13d
   53c36:	jne    540ba <clockcache_get+0x5ea>
   53c3c:	mov    0x20(%rdx),%eax
   53c3f:	cmp    %eax,0x1c(%rsp)
   53c43:	jne    5413a <clockcache_get+0x66a>
   53c49:	mov    0x8(%r14),%rax
   53c4d:	mov    0x18(%rax),%r11d
   53c51:	test   %r11d,%r11d
   53c54:	jne    53f36 <clockcache_get+0x466>
   53c5a:	mov    0x20(%rsp),%rbx
   53c5f:	xor    %r13d,%r13d
   53c62:	mov    %rdx,(%rbx)
   53c65:	data16 lea 0x8133b(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53c6d:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53c75:	mov    (%rax),%rax
   53c78:	mov    %rax,0x8(%rbx)
   53c7c:	add    $0x48,%rsp
   53c80:	mov    %r13d,%eax
   53c83:	pop    %rbx
   53c84:	pop    %rbp
   53c85:	pop    %r12
   53c87:	pop    %r13
   53c89:	pop    %r14
   53c8b:	pop    %r15
   53c8d:	ret
   53c8e:	xchg   %ax,%ax
   53c90:	mov    %ebp,%esi
   53c92:	mov    %r13,%rdi
   53c95:	call   aa1f0 <clockcache_try_get_read.constprop.0>
   53c9a:	cmp    $0x1,%eax
   53c9d:	jne    53bdd <clockcache_get+0x10d>
   53ca3:	xor    %eax,%eax
   53ca5:	data16 cs nopw 0x0(%rax,%rax,1)
   53cb0:	pause
   53cb2:	mov    %rax,%rdx
   53cb5:	add    $0x1,%rax
   53cb9:	cmp    %rdx,%r14
   53cbc:	jne    53cb0 <clockcache_get+0x1e0>
   53cbe:	jmp    53c90 <clockcache_get+0x1c0>
   53cc0:	call   aa1f0 <clockcache_try_get_read.constprop.0>
   53cc5:	cmp    $0x1,%eax
   53cc8:	je     53d76 <clockcache_get+0x2a6>
   53cce:	cmp    $0x2,%eax
   53cd1:	jne    53be5 <clockcache_get+0x115>
   53cd7:	mov    0x8(%r13),%rax
   53cdb:	xor    %edx,%edx
   53cdd:	mov    %rbx,%rsi
   53ce0:	mov    0x8(%rax),%rdi
   53ce4:	mov    0x120(%rax),%rcx
   53ceb:	mov    %rbx,%rax
   53cee:	divq   0x8(%rdi)
   53cf2:	shr    %cl,%rsi
   53cf5:	mov    %rsi,%rcx
   53cf8:	test   %rdx,%rdx
   53cfb:	jne    5416b <clockcache_get+0x69b>
   53d01:	mov    0x28(%r13),%rax
   53d05:	shr    $0x2,%rax
   53d09:	cmp    %rax,%rsi
   53d0c:	jb     53b50 <clockcache_get+0x80>
   53d12:	mov    %r13,%r14
   53d15:	jmp    101b7 <clockcache_get.cold>
   53d1a:	nopw   0x0(%rax,%rax,1)
   53d20:	mov    0x10(%r13),%rbp
   53d24:	mov    0x0(%rbp),%rax
   53d28:	mov    %rbp,%rdi
   53d2b:	call   *(%rax)
   53d2d:	xor    %edx,%edx
   53d2f:	mov    %rax,%rcx
   53d32:	mov    (%rax),%rax
   53d35:	mov    0x8(%rax),%r9
   53d39:	mov    %rbx,%rax
   53d3c:	div    %r9
   53d3f:	test   %rdx,%rdx
   53d42:	jne    53f00 <clockcache_get+0x430>
   53d48:	mov    0x0(%rbp),%rax
   53d4c:	mov    %rbx,%r15
   53d4f:	and    0x20(%rcx),%r15
   53d53:	je     53d88 <clockcache_get+0x2b8>
   53d55:	cmp    0x8(%rcx),%rbx
   53d59:	jae    53d88 <clockcache_get+0x2b8>
   53d5b:	mov    %r15,%rsi
   53d5e:	mov    %rbp,%rdi
   53d61:	call   *0x20(%rax)
   53d64:	test   %eax,%eax
   53d66:	je     53fb0 <clockcache_get+0x4e0>
   53d6c:	mov    0x8(%rsp),%r8d
   53d71:	test   %r8d,%r8d
   53d74:	jne    53dd4 <clockcache_get+0x304>
   53d76:	mov    $0xb,%r13d
   53d7c:	jmp    53c7c <clockcache_get+0x1ac>
   53d81:	nopl   0x0(%rax)
   53d88:	mov    %rbp,%rdi
   53d8b:	call   *0x58(%rax)
   53d8e:	mov    0x8123b(%rip),%rbp        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   53d95:	mov    %rbx,%r9
   53d98:	mov    %r15,%r8
   53d9b:	sub    $0x8,%rsp
   53d9f:	lea    0x637ba(%rip),%rsi        # b7560 <__func__.21+0xd0>
   53da6:	mov    $0x17d,%ecx
   53dab:	mov    0x0(%rbp),%rdi
   53daf:	push   %rax
   53db0:	lea    0x6eaf9(%rip),%rdx        # c28b0 <__func__.22.lto_priv.0>
   53db7:	xor    %eax,%eax
   53db9:	call   fbf0 <fprintf@plt>
   53dbe:	mov    0x0(%rbp),%rdi
   53dc2:	call   fe90 <fflush@plt>
   53dc7:	pop    %rsi
   53dc8:	pop    %rdi
   53dc9:	mov    $0x16,%r13d
   53dcf:	jmp    53c7c <clockcache_get+0x1ac>
   53dd4:	data16 lea 0x811cc(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53ddc:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53de4:	mov    0x1c(%rsp),%edx
   53de8:	mov    %rbx,%rsi
   53deb:	mov    %r13,%rdi
   53dee:	mov    (%rax),%r14
   53df1:	mov    0x8(%r13),%rax
   53df5:	mov    0x8(%rax),%rax
   53df9:	mov    0x8(%rax),%r15
   53dfd:	call   53770 <clockcache_acquire_entry_for_load.lto_priv.0>
   53e02:	mov    %rax,%rbp
   53e05:	mov    $0xffffffff,%eax
   53e0a:	cmp    %rax,%rbp
   53e0d:	je     53cd7 <clockcache_get+0x207>
   53e13:	mov    0x8(%r13),%rax
   53e17:	mov    %rbp,%r12
   53e1a:	mov    %r14,0x8(%rsp)
   53e1f:	mov    %r13,%r14
   53e22:	shl    $0x6,%r12
   53e26:	add    0x48(%r13),%r12
   53e2a:	mov    0x18(%rax),%ecx
   53e2d:	test   %ecx,%ecx
   53e2f:	jne    5404e <clockcache_get+0x57e>
   53e35:	mov    0x18(%r14),%rdi
   53e39:	mov    (%r12),%rsi
   53e3d:	mov    %rbx,%rcx
   53e40:	mov    %r15,%rdx
   53e43:	mov    (%rdi),%rax
   53e46:	call   *(%rax)
   53e48:	mov    %eax,%r13d
   53e4b:	test   %eax,%eax
   53e4d:	jne    54075 <clockcache_get+0x5a5>
   53e53:	mov    0x8(%r14),%rax
   53e57:	mov    0x18(%rax),%edx
   53e5a:	test   %edx,%edx
   53e5c:	jne    53ff1 <clockcache_get+0x521>
   53e62:	mov    %ebp,%edx
   53e64:	mov    %r13d,%ecx
   53e67:	mov    %rbx,%rsi
   53e6a:	mov    %r14,%rdi
   53e6d:	call   538a0 <clockcache_finish_load.lto_priv.0>
   53e72:	mov    %r12,%rdx
   53e75:	test   %r13d,%r13d
   53e78:	je     53c5a <clockcache_get+0x18a>
   53e7e:	mov    0x8(%r14),%rcx
   53e82:	mov    %rbp,%rax
   53e85:	xor    %edx,%edx
   53e87:	mov    0x8(%rsp),%rdi
   53e8c:	divq   0x140(%rcx)
   53e93:	and    $0x3,%edi
   53e96:	shl    $0x6,%edx
   53e99:	lea    (%rdx,%rax,1),%esi
   53e9c:	mov    0x130(%rcx),%eax
   53ea2:	mov    0x88(%r14),%rdx
   53ea9:	mov    $0xffffffff,%ecx
   53eae:	imul   %rdi,%rax
   53eb2:	add    %rsi,%rax
   53eb5:	lock xadd %cx,(%rdx,%rax,2)
   53ebb:	test   %cx,%cx
   53ebe:	jne    53c7c <clockcache_get+0x1ac>
   53ec4:	push   %rax
   53ec5:	push   %rdi
   53ec6:	push   %rsi
   53ec7:	push   0x20(%rsp)
   53ecb:	xor    %r9d,%r9d
   53ece:	lea    0x64323(%rip),%r8        # b81f8 <__func__.21+0xd68>
   53ed5:	xor    %eax,%eax
   53ed7:	mov    $0x284,%esi
   53edc:	lea    0x5d74b(%rip),%rcx        # b162e <_fini+0x952>
   53ee3:	lea    0x6eb06(%rip),%rdx        # c29f0 <__func__.28.lto_priv.1>
   53eea:	lea    0x5d6e6(%rip),%rdi        # b15d7 <_fini+0x8fb>
   53ef1:	call   ee00 <platform_assert_false@plt>
   53ef6:	cs nopw 0x0(%rax,%rax,1)
   53f00:	mov    0x810c9(%rip),%rbp        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   53f07:	mov    %rbx,%r8
   53f0a:	xor    %eax,%eax
   53f0c:	mov    $0x164,%ecx
   53f11:	lea    0x6e998(%rip),%rdx        # c28b0 <__func__.22.lto_priv.0>
   53f18:	lea    0x63589(%rip),%rsi        # b74a8 <__func__.21+0x18>
   53f1f:	mov    0x0(%rbp),%rdi
   53f23:	call   fbf0 <fprintf@plt>
   53f28:	mov    0x0(%rbp),%rdi
   53f2c:	call   fe90 <fflush@plt>
   53f31:	jmp    53dc9 <clockcache_get+0x2f9>
   53f36:	mov    0x10(%rsp),%rbx
   53f3b:	mov    0x1c(%rsp),%ecx
   53f3f:	lea    0x0(,%rbx,8),%rax
   53f47:	sub    %rbx,%rax
   53f4a:	lea    0x220(%rcx,%rax,8),%rax
   53f52:	addq   $0x1,(%r14,%rax,8)
   53f57:	jmp    53c5a <clockcache_get+0x18a>
   53f5c:	mov    0x8(%r13),%rcx
   53f60:	xor    %edx,%edx
   53f62:	mov    0x10(%rsp),%rdi
   53f67:	divq   0x140(%rcx)
   53f6e:	and    $0x3,%edi
   53f71:	shl    $0x6,%edx
   53f74:	lea    (%rdx,%rax,1),%esi
   53f77:	mov    0x130(%rcx),%eax
   53f7d:	mov    0x88(%r13),%rdx
   53f84:	mov    $0xffffffff,%ecx
   53f89:	imul   %rdi,%rax
   53f8d:	add    %rsi,%rax
   53f90:	lock xadd %cx,(%rdx,%rax,2)
   53f96:	test   %cx,%cx
   53f99:	jne    53cd7 <clockcache_get+0x207>
   53f9f:	push   %rbx
   53fa0:	push   %rdi
   53fa1:	push   %rsi
   53fa2:	push   0x28(%rsp)
   53fa6:	jmp    53ecb <clockcache_get+0x3fb>
   53fab:	nopl   0x0(%rax,%rax,1)
   53fb0:	sub    $0x8,%rsp
   53fb4:	mov    %rbx,%r9
   53fb7:	mov    %r15,%r8
   53fba:	mov    $0x172,%ecx
   53fbf:	mov    0x8100a(%rip),%rbp        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   53fc6:	lea    0x6e8e3(%rip),%rdx        # c28b0 <__func__.22.lto_priv.0>
   53fcd:	lea    0x63524(%rip),%rsi        # b74f8 <__func__.21+0x68>
   53fd4:	mov    0x0(%rbp),%rdi
   53fd8:	push   $0x0
   53fda:	call   fbf0 <fprintf@plt>
   53fdf:	mov    0x0(%rbp),%rdi
   53fe3:	call   fe90 <fflush@plt>
   53fe8:	pop    %r9
   53fea:	pop    %r10
   53fec:	jmp    53dc9 <clockcache_get+0x2f9>
   53ff1:	mov    $0x1,%edi
   53ff6:	lea    0x30(%rsp),%rsi
   53ffb:	call   e410 <clock_gettime@plt>
   54000:	mov    0x28(%rsp),%rax
   54005:	mov    0x8(%rsp),%rdi
   5400a:	imul   $0x3b9aca00,0x30(%rsp),%rdx
   54013:	add    0x38(%rsp),%rdx
   54018:	sub    %rax,%rdx
   5401b:	mov    0x1c(%rsp),%ecx
   5401f:	lea    0x0(,%rdi,8),%rax
   54027:	sub    %rdi,%rax
   5402a:	lea    (%rcx,%rax,8),%rax
   5402e:	lea    (%r14,%rax,8),%rax
   54032:	addq   $0x1,0x1148(%rax)
   5403a:	addq   $0x1,0x1220(%rax)
   54042:	add    %rdx,0x1190(%rax)
   54049:	jmp    53e62 <clockcache_get+0x392>
   5404e:	lea    0x30(%rsp),%rsi
   54053:	mov    $0x1,%edi
   54058:	call   e410 <clock_gettime@plt>
   5405d:	imul   $0x3b9aca00,0x30(%rsp),%rax
   54066:	add    0x38(%rsp),%rax
   5406b:	mov    %rax,0x28(%rsp)
   54070:	jmp    53e35 <clockcache_get+0x365>
   54075:	mov    %eax,%edi
   54077:	call   efe0 <strerror@plt>
   5407c:	mov    0x80f4d(%rip),%r15        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   54083:	mov    0x1c(%rsp),%ecx
   54087:	mov    %rbx,%rdx
   5408a:	mov    %rax,%r8
   5408d:	lea    0x6480c(%rip),%rsi        # b88a0 <__func__.21+0x1410>
   54094:	xor    %eax,%eax
   54096:	mov    (%r15),%rdi
   54099:	call   fbf0 <fprintf@plt>
   5409e:	mov    (%r15),%rdi
   540a1:	call   fe90 <fflush@plt>
   540a6:	mov    0x8(%r14),%rax
   540aa:	mov    0x18(%rax),%edx
   540ad:	test   %edx,%edx
   540af:	je     53e62 <clockcache_get+0x392>
   540b5:	jmp    53ff1 <clockcache_get+0x521>
   540ba:	mov    0x8(%r14),%rcx
   540be:	xor    %edx,%edx
   540c0:	mov    0x10(%rsp),%rdi
   540c5:	divq   0x140(%rcx)
   540cc:	and    $0x3,%edi
   540cf:	shl    $0x6,%edx
   540d2:	lea    (%rdx,%rax,1),%esi
   540d5:	mov    0x130(%rcx),%eax
   540db:	mov    0x88(%r14),%rdx
   540e2:	mov    $0xffffffff,%ecx
   540e7:	imul   %rdi,%rax
   540eb:	add    %rsi,%rax
   540ee:	lock xadd %cx,(%rdx,%rax,2)
   540f4:	test   %cx,%cx
   540f7:	jne    53c7c <clockcache_get+0x1ac>
   540fd:	jmp    53f9f <clockcache_get+0x4cf>
   54102:	nopw   0x0(%rax,%rax,1)
   54108:	sub    $0x8,%rsp
   5410c:	lea    0x6452d(%rip),%r8        # b8640 <__func__.21+0x11b0>
   54113:	xor    %eax,%eax
   54115:	push   %rsi
   54116:	mov    0x2c(%rsp),%r9d
   5411b:	lea    0x64546(%rip),%rcx        # b8668 <__func__.21+0x11d8>
   54122:	lea    0x6e757(%rip),%rdx        # c2880 <__func__.23.lto_priv.1>
   54129:	mov    $0x1d2,%esi
   5412e:	lea    0x5d4a2(%rip),%rdi        # b15d7 <_fini+0x8fb>
   54135:	call   ee00 <platform_assert_false@plt>
   5413a:	mov    0x1c(%rsp),%ebx
   5413e:	mov    %ebp,%r9d
   54141:	lea    0x5d5c2(%rip),%r8        # b170a <_fini+0xa2e>
   54148:	lea    0x5d5d2(%rip),%rcx        # b1721 <_fini+0xa45>
   5414f:	lea    0x6e63a(%rip),%rdx        # c2790 <__func__.19>
   54156:	mov    $0x8d9,%esi
   5415b:	lea    0x5d475(%rip),%rdi        # b15d7 <_fini+0x8fb>
   54162:	push   %rbx
   54163:	push   %rax
   54164:	xor    %eax,%eax
   54166:	call   ee00 <platform_assert_false@plt>
   5416b:	mov    %r13,%r14
   5416e:	jmp    101b7 <clockcache_get.cold>

Disassembly of section .fini:
