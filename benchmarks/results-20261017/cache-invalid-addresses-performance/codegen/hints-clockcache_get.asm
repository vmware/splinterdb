
.cache/invalid-address-perf-20261017/builds/hints/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000053c70 <clockcache_get>:
   53c70:	push   %r15
   53c72:	lea    -0x1(%rcx),%eax
   53c75:	push   %r14
   53c77:	push   %r13
   53c79:	push   %r12
   53c7b:	push   %rbp
   53c7c:	push   %rbx
   53c7d:	mov    %rsi,%rbx
   53c80:	sub    $0x38,%rsp
   53c84:	mov    %ecx,0xc(%rsp)
   53c88:	mov    %r8,0x10(%rsp)
   53c8d:	cmp    $0x7,%eax
   53c90:	ja     542b7 <clockcache_get+0x647>
   53c96:	mov    0x8(%rdi),%rcx
   53c9a:	mov    %edx,%esi
   53c9c:	xor    %edx,%edx
   53c9e:	mov    0x8(%rcx),%rax
   53ca2:	mov    0x8(%rax),%r8
   53ca6:	mov    %rbx,%rax
   53ca9:	div    %r8
   53cac:	test   %rdx,%rdx
   53caf:	jne    53e94 <clockcache_get+0x224>
   53cb5:	mov    %esi,0x8(%rsp)
   53cb9:	mov    %rdi,%r13
   53cbc:	movabs $0xcccccccccccccccd,%rbp
   53cc6:	cs nopw 0x0(%rax,%rax,1)
   53cd0:	mov    0x120(%rcx),%rcx
   53cd7:	mov    0x28(%r13),%rax
   53cdb:	mov    %rbx,%rdx
   53cde:	shr    %cl,%rdx
   53ce1:	shr    $0x2,%rax
   53ce5:	cmp    %rax,%rdx
   53ce8:	jae    53f80 <clockcache_get+0x310>
   53cee:	mov    0x30(%r13),%rax
   53cf2:	mov    (%rax,%rdx,4),%r14d
   53cf6:	cmp    $0xffffffff,%r14d
   53cfa:	je     53ed0 <clockcache_get+0x260>
   53d00:	data16 lea 0x822a0(%rip),%rdi        # d5fa8 <xxxtid@@Base+0xd5fa8>
   53d08:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53d10:	mov    0x8(%rsp),%r12d
   53d15:	mov    %r14d,%esi
   53d18:	mov    %r13,%rdi
   53d1b:	mov    (%rax),%rax
   53d1e:	mov    %rax,(%rsp)
   53d22:	test   %r12d,%r12d
   53d25:	je     53e60 <clockcache_get+0x1f0>
   53d2b:	call   aa4f0 <clockcache_try_get_read.constprop.0>
   53d30:	cmp    $0x1,%eax
   53d33:	jne    53d7e <clockcache_get+0x10e>
   53d35:	mov    $0x1,%r15d
   53d3b:	mov    %r15,%rax
   53d3e:	mul    %rbp
   53d41:	xor    %eax,%eax
   53d43:	shr    $0x2,%rdx
   53d47:	mov    %rdx,%r12
   53d4a:	nopw   0x0(%rax,%rax,1)
   53d50:	pause
   53d52:	mov    %rax,%rcx
   53d55:	add    $0x1,%rax
   53d59:	cmp    %r12,%rcx
   53d5c:	jne    53d50 <clockcache_get+0xe0>
   53d5e:	cmp    $0x400,%r15
   53d65:	ja     53e38 <clockcache_get+0x1c8>
   53d6b:	mov    %r14d,%esi
   53d6e:	mov    %r13,%rdi
   53d71:	add    %r15,%r15
   53d74:	call   aa4f0 <clockcache_try_get_read.constprop.0>
   53d79:	cmp    $0x1,%eax
   53d7c:	je     53d3b <clockcache_get+0xcb>
   53d7e:	test   %eax,%eax
   53d80:	jne    53e77 <clockcache_get+0x207>
   53d86:	mov    %r14d,%eax
   53d89:	mov    0x48(%r13),%rcx
   53d8d:	mov    %rax,%rdx
   53d90:	shl    $0x6,%rdx
   53d94:	mov    0x8(%rcx,%rdx,1),%rsi
   53d99:	cmp    %rsi,%rbx
   53d9c:	jne    5413b <clockcache_get+0x4cb>
   53da2:	mov    %r13,%r12
   53da5:	mov    %rax,%rbx
   53da8:	mov    %rdx,%r13
   53dab:	jmp    53dbd <clockcache_get+0x14d>
   53dad:	nopl   (%rax)
   53db0:	mov    %r12,%rdi
   53db3:	call   ea40 <clockcache_wait@plt>
   53db8:	mov    0x48(%r12),%rcx
   53dbd:	mov    0x10(%rcx,%r13,1),%eax
   53dc2:	test   $0x10,%al
   53dc4:	jne    53db0 <clockcache_get+0x140>
   53dc6:	mov    0x48(%r12),%rdx
   53dcb:	mov    %rbx,%rax
   53dce:	add    %r13,%rdx
   53dd1:	mov    0x14(%rdx),%r9d
   53dd5:	test   %r9d,%r9d
   53dd8:	jne    542ad <clockcache_get+0x63d>
   53dde:	mov    0x20(%rdx),%eax
   53de1:	cmp    %eax,0xc(%rsp)
   53de5:	jne    542e9 <clockcache_get+0x679>
   53deb:	mov    0x8(%r12),%rax
   53df0:	mov    0x18(%rax),%r11d
   53df4:	test   %r11d,%r11d
   53df7:	jne    54116 <clockcache_get+0x4a6>
   53dfd:	mov    0x10(%rsp),%rbx
   53e02:	mov    %rdx,(%rbx)
   53e05:	data16 lea 0x8219b(%rip),%rdi        # d5fa8 <xxxtid@@Base+0xd5fa8>
   53e0d:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53e15:	xor    %r9d,%r9d
   53e18:	mov    (%rax),%rax
   53e1b:	mov    %rax,0x8(%rbx)
   53e1f:	add    $0x38,%rsp
   53e23:	mov    %r9d,%eax
   53e26:	pop    %rbx
   53e27:	pop    %rbp
   53e28:	pop    %r12
   53e2a:	pop    %r13
   53e2c:	pop    %r14
   53e2e:	pop    %r15
   53e30:	ret
   53e31:	nopl   0x0(%rax)
   53e38:	mov    %r14d,%esi
   53e3b:	mov    %r13,%rdi
   53e3e:	call   aa4f0 <clockcache_try_get_read.constprop.0>
   53e43:	cmp    $0x1,%eax
   53e46:	jne    53d7e <clockcache_get+0x10e>
   53e4c:	xor    %eax,%eax
   53e4e:	xchg   %ax,%ax
   53e50:	pause
   53e52:	mov    %rax,%rdx
   53e55:	add    $0x1,%rax
   53e59:	cmp    %rdx,%r12
   53e5c:	jne    53e50 <clockcache_get+0x1e0>
   53e5e:	jmp    53e38 <clockcache_get+0x1c8>
   53e60:	call   aa4f0 <clockcache_try_get_read.constprop.0>
   53e65:	cmp    $0x1,%eax
   53e68:	je     53f28 <clockcache_get+0x2b8>
   53e6e:	cmp    $0x2,%eax
   53e71:	jne    53d86 <clockcache_get+0x116>
   53e77:	mov    0x8(%r13),%rcx
   53e7b:	xor    %edx,%edx
   53e7d:	mov    0x8(%rcx),%rax
   53e81:	mov    0x8(%rax),%r8
   53e85:	mov    %rbx,%rax
   53e88:	div    %r8
   53e8b:	test   %rdx,%rdx
   53e8e:	je     53cd0 <clockcache_get+0x60>
   53e94:	mov    0x82135(%rip),%rbp        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   53e9b:	mov    %r8,%rcx
   53e9e:	mov    %rbx,%rdx
   53ea1:	xor    %eax,%eax
   53ea3:	lea    0x65406(%rip),%rsi        # b92b0 <__func__.21+0xe20>
   53eaa:	mov    0x0(%rbp),%rdi
   53eae:	call   fbf0 <fprintf@plt>
   53eb3:	mov    0x0(%rbp),%rdi
   53eb7:	call   fe90 <fflush@plt>
   53ebc:	mov    $0x16,%r9d
   53ec2:	jmp    53e1f <clockcache_get+0x1af>
   53ec7:	nopw   0x0(%rax,%rax,1)
   53ed0:	mov    0x10(%r13),%r14
   53ed4:	mov    (%r14),%rax
   53ed7:	mov    %r14,%rdi
   53eda:	call   *(%rax)
   53edc:	xor    %edx,%edx
   53ede:	mov    %rax,%rcx
   53ee1:	mov    (%rax),%rax
   53ee4:	mov    0x8(%rax),%r9
   53ee8:	mov    %rbx,%rax
   53eeb:	div    %r9
   53eee:	test   %rdx,%rdx
   53ef1:	jne    540e0 <clockcache_get+0x470>
   53ef7:	mov    (%r14),%rax
   53efa:	mov    %rbx,%r15
   53efd:	and    0x20(%rcx),%r15
   53f01:	je     53f38 <clockcache_get+0x2c8>
   53f03:	cmp    0x8(%rcx),%rbx
   53f07:	jae    53f38 <clockcache_get+0x2c8>
   53f09:	mov    %r15,%rsi
   53f0c:	mov    %r14,%rdi
   53f0f:	call   *0x20(%rax)
   53f12:	test   %eax,%eax
   53f14:	je     54190 <clockcache_get+0x520>
   53f1a:	mov    0x8(%rsp),%r8d
   53f1f:	test   %r8d,%r8d
   53f22:	jne    53fb1 <clockcache_get+0x341>
   53f28:	mov    $0xb,%r9d
   53f2e:	jmp    53e1f <clockcache_get+0x1af>
   53f33:	nopl   0x0(%rax,%rax,1)
   53f38:	mov    %r14,%rdi
   53f3b:	call   *0x58(%rax)
   53f3e:	mov    0x8208b(%rip),%rbp        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   53f45:	mov    %rbx,%r9
   53f48:	mov    %r15,%r8
   53f4b:	sub    $0x8,%rsp
   53f4f:	lea    0x6460a(%rip),%rsi        # b8560 <__func__.21+0xd0>
   53f56:	mov    $0x17d,%ecx
   53f5b:	mov    0x0(%rbp),%rdi
   53f5f:	push   %rax
   53f60:	lea    0x6f949(%rip),%rdx        # c38b0 <__func__.22.lto_priv.0>
   53f67:	xor    %eax,%eax
   53f69:	call   fbf0 <fprintf@plt>
   53f6e:	mov    0x0(%rbp),%rdi
   53f72:	call   fe90 <fflush@plt>
   53f77:	pop    %rsi
   53f78:	pop    %rdi
   53f79:	jmp    53ebc <clockcache_get+0x24c>
   53f7e:	xchg   %ax,%ax
   53f80:	mov    0x82049(%rip),%rbp        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   53f87:	mov    %r8,%rcx
   53f8a:	mov    %rbx,%rdx
   53f8d:	lea    0x65354(%rip),%rsi        # b92e8 <__func__.21+0xe58>
   53f94:	imul   %rax,%rcx
   53f98:	xor    %eax,%eax
   53f9a:	mov    0x0(%rbp),%rdi
   53f9e:	call   fbf0 <fprintf@plt>
   53fa3:	mov    0x0(%rbp),%rdi
   53fa7:	call   fe90 <fflush@plt>
   53fac:	jmp    53ebc <clockcache_get+0x24c>
   53fb1:	data16 lea 0x81fef(%rip),%rdi        # d5fa8 <xxxtid@@Base+0xd5fa8>
   53fb9:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53fc1:	mov    0xc(%rsp),%edx
   53fc5:	mov    %rbx,%rsi
   53fc8:	mov    %r13,%rdi
   53fcb:	mov    (%rax),%r12
   53fce:	mov    0x8(%r13),%rax
   53fd2:	mov    0x8(%rax),%rax
   53fd6:	mov    0x8(%rax),%r15
   53fda:	call   53840 <clockcache_acquire_entry_for_load.lto_priv.0>
   53fdf:	mov    %rax,%r14
   53fe2:	mov    $0xffffffff,%eax
   53fe7:	cmp    %rax,%r14
   53fea:	je     53e77 <clockcache_get+0x207>
   53ff0:	mov    0x8(%r13),%rax
   53ff4:	mov    %r14,%rbp
   53ff7:	mov    %r12,(%rsp)
   53ffb:	mov    %r13,%r12
   53ffe:	shl    $0x6,%rbp
   54002:	add    0x48(%r13),%rbp
   54006:	mov    0x18(%rax),%ecx
   54009:	test   %ecx,%ecx
   5400b:	jne    54237 <clockcache_get+0x5c7>
   54011:	mov    0x18(%r12),%rdi
   54016:	mov    0x0(%rbp),%rsi
   5401a:	mov    %rbx,%rcx
   5401d:	mov    %r15,%rdx
   54020:	mov    (%rdi),%rax
   54023:	call   *(%rax)
   54025:	mov    %eax,%r9d
   54028:	test   %eax,%eax
   5402a:	jne    5425e <clockcache_get+0x5ee>
   54030:	mov    0x8(%r12),%rax
   54035:	mov    0x18(%rax),%edx
   54038:	test   %edx,%edx
   5403a:	jne    541d1 <clockcache_get+0x561>
   54040:	mov    %r9d,%ecx
   54043:	mov    %r14d,%edx
   54046:	mov    %rbx,%rsi
   54049:	mov    %r12,%rdi
   5404c:	mov    %r9d,0x8(%rsp)
   54051:	call   539d0 <clockcache_finish_load.lto_priv.0>
   54056:	mov    0x8(%rsp),%r9d
   5405b:	mov    %rbp,%rdx
   5405e:	test   %r9d,%r9d
   54061:	je     53dfd <clockcache_get+0x18d>
   54067:	mov    0x8(%r12),%rcx
   5406c:	mov    %r14,%rax
   5406f:	xor    %edx,%edx
   54071:	mov    (%rsp),%rdi
   54075:	divq   0x140(%rcx)
   5407c:	and    $0x3,%edi
   5407f:	shl    $0x6,%edx
   54082:	lea    (%rdx,%rax,1),%esi
   54085:	mov    0x130(%rcx),%eax
   5408b:	mov    $0xffffffff,%ecx
   54090:	mov    0x88(%r12),%rdx
   54098:	imul   %rdi,%rax
   5409c:	add    %rsi,%rax
   5409f:	lock xadd %cx,(%rdx,%rax,2)
   540a5:	test   %cx,%cx
   540a8:	jne    53e1f <clockcache_get+0x1af>
   540ae:	push   %rax
   540af:	push   %rdi
   540b0:	push   %rsi
   540b1:	push   0x18(%rsp)
   540b5:	xor    %r9d,%r9d
   540b8:	lea    0x650c9(%rip),%r8        # b9188 <__func__.21+0xcf8>
   540bf:	xor    %eax,%eax
   540c1:	lea    0x5e566(%rip),%rcx        # b262e <_fini+0x1512>
   540c8:	lea    0x6f921(%rip),%rdx        # c39f0 <__func__.28.lto_priv.1>
   540cf:	mov    $0x27c,%esi
   540d4:	lea    0x5e4fc(%rip),%rdi        # b25d7 <_fini+0x14bb>
   540db:	call   ee00 <platform_assert_false@plt>
   540e0:	mov    0x81ee9(%rip),%rbp        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   540e7:	mov    %rbx,%r8
   540ea:	xor    %eax,%eax
   540ec:	mov    $0x164,%ecx
   540f1:	lea    0x6f7b8(%rip),%rdx        # c38b0 <__func__.22.lto_priv.0>
   540f8:	lea    0x643a9(%rip),%rsi        # b84a8 <__func__.21+0x18>
   540ff:	mov    0x0(%rbp),%rdi
   54103:	call   fbf0 <fprintf@plt>
   54108:	mov    0x0(%rbp),%rdi
   5410c:	call   fe90 <fflush@plt>
   54111:	jmp    53ebc <clockcache_get+0x24c>
   54116:	mov    (%rsp),%rsi
   5411a:	mov    0xc(%rsp),%ecx
   5411e:	lea    0x0(,%rsi,8),%rax
   54126:	sub    %rsi,%rax
   54129:	lea    0x220(%rcx,%rax,8),%rax
   54131:	addq   $0x1,(%r12,%rax,8)
   54136:	jmp    53dfd <clockcache_get+0x18d>
   5413b:	mov    0x8(%r13),%rcx
   5413f:	xor    %edx,%edx
   54141:	mov    (%rsp),%r8
   54145:	divq   0x140(%rcx)
   5414c:	and    $0x3,%r8d
   54150:	shl    $0x6,%edx
   54153:	lea    (%rdx,%rax,1),%edi
   54156:	mov    0x130(%rcx),%eax
   5415c:	mov    0x88(%r13),%rdx
   54163:	mov    $0xffffffff,%ecx
   54168:	imul   %r8,%rax
   5416c:	add    %rdi,%rax
   5416f:	lock xadd %cx,(%rdx,%rax,2)
   54175:	test   %cx,%cx
   54178:	jne    53e77 <clockcache_get+0x207>
   5417e:	push   %rbx
   5417f:	push   %r8
   54181:	push   %rdi
   54182:	jmp    540b1 <clockcache_get+0x441>
   54187:	nopw   0x0(%rax,%rax,1)
   54190:	sub    $0x8,%rsp
   54194:	mov    %rbx,%r9
   54197:	mov    %r15,%r8
   5419a:	mov    $0x172,%ecx
   5419f:	mov    0x81e2a(%rip),%rbp        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   541a6:	lea    0x6f703(%rip),%rdx        # c38b0 <__func__.22.lto_priv.0>
   541ad:	lea    0x64344(%rip),%rsi        # b84f8 <__func__.21+0x68>
   541b4:	mov    0x0(%rbp),%rdi
   541b8:	push   $0x0
   541ba:	call   fbf0 <fprintf@plt>
   541bf:	mov    0x0(%rbp),%rdi
   541c3:	call   fe90 <fflush@plt>
   541c8:	pop    %r9
   541ca:	pop    %r10
   541cc:	jmp    53ebc <clockcache_get+0x24c>
   541d1:	lea    0x20(%rsp),%rsi
   541d6:	mov    $0x1,%edi
   541db:	mov    %r9d,0x8(%rsp)
   541e0:	call   e410 <clock_gettime@plt>
   541e5:	mov    0x18(%rsp),%rax
   541ea:	mov    (%rsp),%rsi
   541ee:	imul   $0x3b9aca00,0x20(%rsp),%rdx
   541f7:	add    0x28(%rsp),%rdx
   541fc:	sub    %rax,%rdx
   541ff:	mov    0xc(%rsp),%ecx
   54203:	lea    0x0(,%rsi,8),%rax
   5420b:	mov    0x8(%rsp),%r9d
   54210:	sub    %rsi,%rax
   54213:	lea    (%rcx,%rax,8),%rax
   54217:	lea    (%r12,%rax,8),%rax
   5421b:	addq   $0x1,0x1148(%rax)
   54223:	addq   $0x1,0x1220(%rax)
   5422b:	add    %rdx,0x1190(%rax)
   54232:	jmp    54040 <clockcache_get+0x3d0>
   54237:	lea    0x20(%rsp),%rsi
   5423c:	mov    $0x1,%edi
   54241:	call   e410 <clock_gettime@plt>
   54246:	imul   $0x3b9aca00,0x20(%rsp),%rax
   5424f:	add    0x28(%rsp),%rax
   54254:	mov    %rax,0x18(%rsp)
   54259:	jmp    54011 <clockcache_get+0x3a1>
   5425e:	mov    %eax,%edi
   54260:	mov    %eax,0x8(%rsp)
   54264:	call   efe0 <strerror@plt>
   54269:	mov    0x81d60(%rip),%r15        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   54270:	mov    0xc(%rsp),%ecx
   54274:	mov    %rbx,%rdx
   54277:	mov    %rax,%r8
   5427a:	lea    0x6561f(%rip),%rsi        # b98a0 <__func__.21+0x1410>
   54281:	xor    %eax,%eax
   54283:	mov    (%r15),%rdi
   54286:	call   fbf0 <fprintf@plt>
   5428b:	mov    (%r15),%rdi
   5428e:	call   fe90 <fflush@plt>
   54293:	mov    0x8(%r12),%rax
   54298:	mov    0x8(%rsp),%r9d
   5429d:	mov    0x18(%rax),%edx
   542a0:	test   %edx,%edx
   542a2:	je     54040 <clockcache_get+0x3d0>
   542a8:	jmp    541d1 <clockcache_get+0x561>
   542ad:	mov    0x8(%r12),%rcx
   542b2:	jmp    5406f <clockcache_get+0x3ff>
   542b7:	sub    $0x8,%rsp
   542bb:	lea    0x6537e(%rip),%r8        # b9640 <__func__.21+0x11b0>
   542c2:	xor    %eax,%eax
   542c4:	push   %rsi
   542c5:	mov    0x1c(%rsp),%r9d
   542ca:	lea    0x65397(%rip),%rcx        # b9668 <__func__.21+0x11d8>
   542d1:	lea    0x6f5a8(%rip),%rdx        # c3880 <__func__.23.lto_priv.1>
   542d8:	mov    $0x1ca,%esi
   542dd:	lea    0x5e2f3(%rip),%rdi        # b25d7 <_fini+0x14bb>
   542e4:	call   ee00 <platform_assert_false@plt>
   542e9:	mov    0xc(%rsp),%esi
   542ed:	mov    %r14d,%r9d
   542f0:	lea    0x5e413(%rip),%r8        # b270a <_fini+0x15ee>
   542f7:	lea    0x5e423(%rip),%rcx        # b2721 <_fini+0x1605>
   542fe:	lea    0x6f48b(%rip),%rdx        # c3790 <__func__.19>
   54305:	lea    0x5e2cb(%rip),%rdi        # b25d7 <_fini+0x14bb>
   5430c:	push   %rsi
   5430d:	mov    $0x8d1,%esi
   54312:	push   %rax
   54313:	xor    %eax,%eax
   54315:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
